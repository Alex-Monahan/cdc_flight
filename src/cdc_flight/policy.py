"""Application-side PII policy gate.

Stock Debezium's column mappers are intentionally not the security boundary: they
are type-selective and key columns bypass the value mapper.  ``PolicyGate`` is the
single post-decode/pre-assembler boundary.  It copies only sanctioned images into a
record and turns the source change-event object into an opaque acknowledgement
handle; no decoded source mapping is retained downstream.

The transform actions use ``PostgreSQLOutputText`` for values whose PostgreSQL
OUTPUT-function representation has been proved by a source read or connector
adapter.  A non-null value without that proof is refused.  In particular this
module never calls ``str(value)``, ``repr(value)``, JSON default conversion, or
``::text`` to manufacture policy input.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from . import naming
from .errors import AdmissionError, SchemaEvolutionRefused
from .toast import field_value
from .typed_types import (
    FieldState,
    FieldValue,
    SourceTypeDescriptor,
    TypedImage,
)

POLICY_VERSION = 1
_ACTIONS = frozenset({"exclude", "mask", "hash", "truncate", "replicate"})
_TEXT_KINDS = frozenset(
    {
        "char", "bpchar", "varchar", "text", "citext", "name", "string",
        "xml", "opaque", "inet", "cidr", "json", "jsonb", "multirange",
    }
)


class PolicyConfigurationError(ValueError):
    """The PII manifest or secret reference is unsafe or incomplete."""


class PolicyValueRefused(AdmissionError):
    """A value crossed the policy boundary without output-function proof."""


class PostgreSQLOutputText(str):
    """Text returned by a PostgreSQL type OUTPUT function.

    This nominal wrapper is deliberately required for non-string connector values.
    It is not a conversion operation: callers obtain the text from PostgreSQL's
    resolved ``typoutput`` function and wrap that already-authoritative result.
    """

    __slots__ = ("output_function_oid",)

    def __new__(cls, value: str, output_function_oid: int | None = None):
        if not isinstance(value, str):
            raise TypeError("PostgreSQLOutputText requires output-function text")
        instance = super().__new__(cls, value)
        instance.output_function_oid = output_function_oid
        return instance


@dataclass(frozen=True, repr=False)
class PIIRule:
    """One fully-qualified column rule."""

    column_regex: str
    action: str
    replacement: str | None = None
    max_chars: int | None = None
    algorithm: str | None = None
    salt_id: str | None = None
    rule_id: str | None = None
    _compiled: re.Pattern = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        pattern = str(self.column_regex)
        if not pattern.startswith("^") or not pattern.endswith("$"):
            raise PolicyConfigurationError(
                "every PII column_regex must be anchored with ^ and $"
            )
        # A qualified rule has three identifier components.  Escaped dots are the
        # normal spelling; the looser count also accepts a deliberate regex in each
        # component while still rejecting a column-only pattern.
        body = pattern[1:-1]
        # Count escaped identifier separators once.  Counting both ``.`` and
        # ``\.`` would accept ``schema\.column`` as a three-part name.
        separator_body = body.replace(r"\.", ".")
        if separator_body.count(".") < 2:
            raise PolicyConfigurationError(
                "every PII column_regex must target schema.table.column"
            )
        try:
            compiled = re.compile(pattern)
        except re.error as exc:
            raise PolicyConfigurationError("PII column_regex is not valid regex") from exc
        action = str(self.action).strip().lower()
        if action not in _ACTIONS:
            raise PolicyConfigurationError(f"unknown PII action {action!r}")
        object.__setattr__(self, "column_regex", pattern)
        object.__setattr__(self, "action", action)
        object.__setattr__(self, "_compiled", compiled)
        if self.rule_id is None:
            object.__setattr__(self, "rule_id", pattern)
        if action == "mask":
            if not isinstance(self.replacement, str) or not self.replacement:
                raise PolicyConfigurationError("mask rules require a non-empty replacement")
            if "\x00" in self.replacement:
                raise PolicyConfigurationError("mask replacement may not contain NUL")
        elif self.replacement is not None:
            raise PolicyConfigurationError("replacement is valid only for mask rules")
        if action == "truncate":
            if isinstance(self.max_chars, bool) or not isinstance(self.max_chars, int):
                raise PolicyConfigurationError("truncate rules require integer max_chars")
            if self.max_chars < 0:
                raise PolicyConfigurationError("truncate max_chars must be non-negative")
        elif self.max_chars is not None:
            raise PolicyConfigurationError("max_chars is valid only for truncate rules")
        if action == "hash":
            if (self.algorithm or "").upper() != "HMAC-SHA-256":
                raise PolicyConfigurationError("hash rules require algorithm HMAC-SHA-256")
            if not self.salt_id or not str(self.salt_id).strip():
                raise PolicyConfigurationError("hash rules require a non-empty salt_id")
        elif self.algorithm is not None or self.salt_id is not None:
            raise PolicyConfigurationError("algorithm/salt_id are valid only for hash rules")

    def matches(self, qualified_column: str) -> bool:
        return self._compiled.fullmatch(qualified_column) is not None

    def safe_dict(self) -> dict[str, Any]:
        result = {
            "column_regex": self.column_regex,
            "action": self.action,
        }
        if self.replacement is not None:
            result["replacement"] = self.replacement
        if self.max_chars is not None:
            result["max_chars"] = self.max_chars
        if self.algorithm is not None:
            result["algorithm"] = self.algorithm
        if self.salt_id is not None:
            result["salt_id"] = self.salt_id
        if self.rule_id is not None:
            result["rule_id"] = self.rule_id
        return result

    def __repr__(self) -> str:
        return f"PIIRule({self.safe_dict()!r})"


def _literal_candidate(rule: PIIRule) -> str | None:
    """Return a useful overlap witness for a literal-ish anchored regex."""
    body = rule.column_regex[1:-1]
    # Regex operators make exact language comparison undecidable here.  Escaped
    # identifier punctuation and ordinary identifier characters are safe witnesses.
    if re.search(r"(?<!\\)[\[\]()*+?|{}]", body):
        return None
    try:
        return re.sub(r"\\(.)", r"\1", body)
    except re.error:  # pragma: no cover - compiled rule already checked
        return None


def _rules_overlap(first: PIIRule, second: PIIRule) -> bool:
    if first.column_regex == second.column_regex:
        return True
    for candidate in (_literal_candidate(first), _literal_candidate(second)):
        if candidate and first.matches(candidate) and second.matches(candidate):
            return True
    return False


def _read_secret(path: str | os.PathLike[str]) -> bytes:
    secret_path = Path(path)
    try:
        info = secret_path.stat()
    except OSError as exc:
        raise PolicyConfigurationError("PII hash salt file is unavailable") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
        raise PolicyConfigurationError("PII hash salt file must be a private regular file")
    try:
        value = secret_path.read_bytes()
    except OSError as exc:
        raise PolicyConfigurationError("PII hash salt file cannot be read") from exc
    if not value:
        raise PolicyConfigurationError("PII hash salt file may not be empty")
    return value.rstrip(b"\n") or value


@dataclass(frozen=True, repr=False)
class PIIPolicy:
    """Compiled, versioned PII policy with process-private salt."""

    rules: tuple[PIIRule, ...] = ()
    unmatched: str = "replicate"
    epoch: int = 0
    salt_id: str | None = field(default=None, repr=False)
    _salt: bytes | None = field(default=None, repr=False, compare=False)
    enabled: bool = True
    digest: str = field(default="", compare=False)

    def __post_init__(self) -> None:
        unmatched = str(self.unmatched).strip().lower()
        if unmatched not in {"exclude", "replicate"}:
            raise PolicyConfigurationError("CDC_PII_UNMATCHED must be exclude or replicate")
        object.__setattr__(self, "unmatched", unmatched)
        ordered = tuple(self.rules)
        for index, rule in enumerate(ordered):
            if not isinstance(rule, PIIRule):
                raise PolicyConfigurationError(f"PII rule {index} is not a compiled rule")
            for prior in ordered[:index]:
                if _rules_overlap(prior, rule) and prior.safe_dict() != rule.safe_dict():
                    raise PolicyConfigurationError(
                        "overlapping PII rules must be byte-for-byte identical"
                    )
        hash_ids = {rule.salt_id for rule in ordered if rule.action == "hash"}
        if hash_ids and (not self._salt or not self.salt_id):
            raise PolicyConfigurationError("hash policy requires a loaded private salt")
        if hash_ids and any(rule.salt_id != self.salt_id for rule in ordered if rule.action == "hash"):
            raise PolicyConfigurationError("all hash rules must use the configured salt_id")
        if int(self.epoch) < 0:
            raise PolicyConfigurationError("PII policy epoch may not be negative")
        object.__setattr__(self, "rules", ordered)
        if not self.digest:
            material = {
                "version": POLICY_VERSION,
                "enabled": bool(self.enabled),
                "unmatched": unmatched,
                "epoch": int(self.epoch),
                "rules": [rule.safe_dict() for rule in ordered],
                "salt_id": self.salt_id,
                # The secret is not persisted, but its fingerprint makes a salt
                # rotation a replay collision rather than a silent reinterpretation.
                "salt_fingerprint": (
                    hashlib.sha256(self._salt).hexdigest() if self._salt else None
                ),
            }
            encoded = json.dumps(material, sort_keys=True, separators=(",", ":"))
            object.__setattr__(self, "digest", hashlib.sha256(encoded.encode()).hexdigest())

    @classmethod
    def disabled(cls) -> PIIPolicy:
        """Compatibility policy for callers with no manifest.

        The gate still marks records as checked and strips the acknowledgement object
        from serialization; a configured manifest switches ``unmatched`` to the
        required fail-closed exclusion policy.
        """
        return cls(unmatched="replicate", epoch=0, enabled=False)

    @classmethod
    def from_manifest(
        cls,
        manifest: str | list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...] | None,
        *,
        unmatched: str = "exclude",
        salt_file: str | os.PathLike[str] | None = None,
        epoch: int = 1,
    ) -> PIIPolicy:
        if manifest is None or manifest == "" or manifest == []:
            return cls.disabled()
        if isinstance(manifest, str):
            try:
                parsed = json.loads(manifest)
            except json.JSONDecodeError as exc:
                raise PolicyConfigurationError("PII rules must be valid JSON") from exc
        else:
            parsed = manifest
        if not isinstance(parsed, (list, tuple)):
            raise PolicyConfigurationError("PII rules must be a JSON array")
        rules: list[PIIRule] = []
        for index, raw in enumerate(parsed):
            if not isinstance(raw, Mapping):
                raise PolicyConfigurationError(f"PII rule {index} must be an object")
            unknown = set(raw) - {
                "column_regex", "action", "replacement", "max_chars", "algorithm",
                "salt_id", "rule_id",
            }
            if unknown:
                raise PolicyConfigurationError("PII rule contains an unknown field")
            try:
                rules.append(
                    PIIRule(
                        column_regex=str(raw["column_regex"]),
                        action=str(raw["action"]),
                        replacement=raw.get("replacement"),
                        max_chars=raw.get("max_chars"),
                        algorithm=raw.get("algorithm"),
                        salt_id=raw.get("salt_id"),
                        rule_id=raw.get("rule_id", f"rule-{index}"),
                    )
                )
            except KeyError as exc:
                raise PolicyConfigurationError("PII rule requires column_regex and action") from exc
        hash_rules = [rule for rule in rules if rule.action == "hash"]
        salt_id = hash_rules[0].salt_id if hash_rules else None
        salt = _read_secret(salt_file) if hash_rules and salt_file else None
        return cls(
            tuple(rules),
            unmatched=unmatched,
            epoch=epoch,
            salt_id=salt_id,
            _salt=salt,
            enabled=True,
        )

    @classmethod
    def from_environment(cls) -> PIIPolicy:
        manifest = os.environ.get("CDC_PII_RULES", "")
        if not manifest.strip():
            return cls.disabled()
        return cls.from_manifest(
            manifest,
            unmatched=os.environ.get("CDC_PII_UNMATCHED", "exclude"),
            salt_file=os.environ.get("CDC_PII_HASH_SALT_FILE") or None,
            epoch=int(os.environ.get("CDC_PII_POLICY_EPOCH", "1")),
        )

    def safe_manifest(self) -> dict[str, Any]:
        return {
            "version": POLICY_VERSION,
            "enabled": self.enabled,
            "unmatched": self.unmatched,
            "epoch": self.epoch,
            "rules": [rule.safe_dict() for rule in self.rules],
            "salt_id": self.salt_id,
            "digest": self.digest,
        }

    def __repr__(self) -> str:
        return f"PIIPolicy({self.safe_manifest()!r})"

    def _canonical_column(self, table: str, column: str) -> str:
        parts = str(table).split(".")
        if len(parts) != 2:
            raise PolicyConfigurationError("policy table context must be schema.table")
        return ".".join((*[naming.normalize(part) for part in parts], naming.normalize(column)))

    def rule_for(self, table: str, column: str) -> PIIRule:
        qualified = self._canonical_column(table, column)
        matches = [rule for rule in self.rules if rule.matches(qualified)]
        if matches:
            return matches[0]
        return PIIRule(
            column_regex=f"^{re.escape(qualified)}$",
            action=self.unmatched,
            rule_id="__unmatched__",
        )

    def descriptor_for_transform(
        self, descriptor: SourceTypeDescriptor, *, action: str, rule_id: str
    ) -> SourceTypeDescriptor:
        return replace(
            descriptor,
            oid=1043,
            qualified_name="pg_catalog.varchar",
            kind="varchar",
            output_function_oid=1043,
            output_function_schema="pg_catalog",
            output_function_name="varcharout",
            domain_base=None,
            array_element=None,
            map_key=None,
            map_value=None,
            composite_fields=(),
            range_subtype=None,
            metadata=tuple(
                sorted(
                    {
                        **dict(descriptor.metadata),
                        "policy_action": action,
                        "policy_rule_id": rule_id,
                        "policy_digest": self.digest,
                    }.items()
                )
            ),
        )

    def _output_text(
        self,
        value: Any,
        descriptor: SourceTypeDescriptor | None,
        *,
        supplied: Any = None,
    ) -> str:
        if value is None:
            return ""
        proof = supplied if supplied is not None else value
        if isinstance(proof, PostgreSQLOutputText):
            if descriptor is None or descriptor.output_function_oid is None:
                raise PolicyValueRefused(
                    "output text has no catalog-resolved PostgreSQL OUTPUT identity"
                )
            if (
                proof.output_function_oid is not None
                and int(proof.output_function_oid) != int(descriptor.output_function_oid)
            ):
                raise PolicyValueRefused(
                    "output text uses a different PostgreSQL OUTPUT function"
                )
            return str(proof)
        metadata = dict(getattr(descriptor, "metadata", ()) or ()) if descriptor else {}
        if (
            metadata.get("output_text_proven") == "postgresql-typoutput"
            and isinstance(proof, str)
        ):
            return proof
        # A catalog-authoritative text value is provable for PostgreSQL's text-like
        # families when the descriptor has its typoutput identity.  Non-text families
        # still require the nominal proof wrapper because Python's runtime shape is
        # not the type output function.
        if (
            descriptor is not None
            and descriptor.output_function_oid is not None
            and str(descriptor.kind).lower() in _TEXT_KINDS
            and isinstance(proof, str)
        ):
            return proof
        raise PolicyValueRefused("non-null policy input lacks PostgreSQL OUTPUT proof")

    def transform(
        self,
        value: Any,
        descriptor: SourceTypeDescriptor | None,
        rule: PIIRule,
        *,
        output_text: Any = None,
    ) -> tuple[Any, SourceTypeDescriptor | None]:
        if value is None:
            return None, self.descriptor_for_transform(
                descriptor or SourceTypeDescriptor(1043, "pg_catalog.varchar", "varchar"),
                action=rule.action,
                rule_id=str(rule.rule_id),
            )
        if rule.action == "mask":
            result = str(rule.replacement)
        elif rule.action in {"hash", "truncate"}:
            text = self._output_text(value, descriptor, supplied=output_text)
            if rule.action == "hash":
                if self._salt is None:
                    raise PolicyValueRefused("hash policy has no process-private salt")
                result = hmac.new(self._salt, text.encode("utf-8"), hashlib.sha256).hexdigest()
            else:
                result = text[: int(rule.max_chars)]
        else:  # pragma: no cover - caller handles action dispatch
            result = value
        return result, self.descriptor_for_transform(
            descriptor or SourceTypeDescriptor(1043, "pg_catalog.varchar", "varchar"),
            action=rule.action,
            rule_id=str(rule.rule_id),
        )

    def sanitize_mapping(
        self,
        table: str,
        mapping: Mapping[str, Any],
        descriptors: Mapping[str, SourceTypeDescriptor] | None = None,
        *,
        output_texts: Mapping[str, Any] | None = None,
        key_columns: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        """Sanitize a source-read row before it becomes a patch or backfill.

        Catalog/backfill reads do not naturally have a ``PendingRecord`` to carry
        through the normal gate. They use this same action compiler and the same
        output-text proof, with the source query supplying ``PostgreSQLOutputText``
        for hash/truncate fields. ``key_columns`` is explicit because excluding a
        source key would make a read-backfill join guess at row identity.
        """
        descriptors = descriptors or {}
        output_texts = output_texts or {}
        normalized_keys = {naming.normalize(column) for column in key_columns}
        result: dict[str, Any] = {}
        for raw_name, value in mapping.items():
            name = naming.normalize(str(raw_name))
            descriptor = descriptors.get(str(raw_name)) or descriptors.get(name)
            rule = self.rule_for(table, name)
            if name in normalized_keys and rule.action != "replicate":
                raise PolicyValueRefused(
                    "a transformed or excluded source key cannot identify a "
                    "catalog backfill row"
                )
            if rule.action == "exclude":
                continue
            if descriptor is None and rule.action != "mask":
                raise PolicyValueRefused("source-read column has no catalog descriptor")
            supplied = output_texts.get(str(raw_name), output_texts.get(name))
            if rule.action in {"mask", "hash", "truncate"}:
                try:
                    value, _descriptor = self.transform(
                        value,
                        descriptor,
                        rule,
                        output_text=supplied,
                    )
                except PolicyValueRefused:
                    # money/xml are an explicit VARCHAR transport carve-out.  A
                    # missing source OUTPUT proof may omit that cell, but it may
                    # never turn into a table-wide schema refusal.  A key remains
                    # fatal above because omission would destroy row identity.
                    if (
                        descriptor is not None
                        and str(descriptor.kind).lower() in {"money", "xml"}
                        and name not in normalized_keys
                    ):
                        continue
                    raise
            result[name] = value
        return result


class AcknowledgementHandle:
    """Opaque, non-serializable holder for a connector acknowledgement token."""

    __slots__ = ("_delegate",)

    def __init__(self, delegate: Any):
        self._delegate = delegate

    def consume(self) -> Any:
        delegate, self._delegate = self._delegate, None
        return delegate

    def __getstate__(self):  # pragma: no cover - exercised by policy tests
        raise TypeError("acknowledgement handles are process-local and non-serializable")

    def __repr__(self) -> str:
        return "<acknowledgement-handle>"


def _descriptor_map(event, descriptor_context) -> dict[str, SourceTypeDescriptor]:
    result: dict[str, SourceTypeDescriptor] = {}
    for attribute in ("key_descriptors", "before_descriptors", "after_descriptors"):
        result.update(getattr(event, attribute, {}) or {})
    if isinstance(descriptor_context, Mapping):
        result.update(descriptor_context)
    return {naming.normalize(str(name)): descriptor for name, descriptor in result.items()}


def _source_output_for(event, image_name: str, column: str) -> Any:
    values = getattr(event, "output_texts", None) or {}
    if isinstance(values, Mapping):
        scoped = values.get(image_name, values)
        if isinstance(scoped, Mapping):
            if column in scoped:
                return scoped[column]
            normalized = naming.normalize(column)
            if normalized in scoped:
                return scoped[normalized]
    return None


class PolicyGate:
    """Sanitize records before they enter assembler, fold, spill, or destination."""

    def __init__(self, policy: PIIPolicy | None = None):
        self.policy = policy or PIIPolicy.disabled()

    def sanitize_mapping(
        self,
        table: str,
        mapping: Mapping[str, Any],
        descriptors: Mapping[str, SourceTypeDescriptor] | None = None,
        *,
        output_texts: Mapping[str, Any] | None = None,
        key_columns: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        """Apply the same gate to snapshot/backfill rows without an envelope."""
        return self.policy.sanitize_mapping(
            table,
            mapping,
            descriptors,
            output_texts=output_texts,
            key_columns=key_columns,
        )

    def sanitize(self, event, descriptor_context=None):
        if not getattr(event, "is_data", False):
            return event
        if getattr(event, "sanitized", False):
            return self.revalidate(event, descriptor_context)
        table = event.qualified_table
        if not table:
            return event
        descriptors = _descriptor_map(event, descriptor_context)
        # Every image is copied.  No downstream object shares a dict with the decoded
        # connector envelope, even when the policy is disabled.
        for image_name, descriptor_name in (
            ("key", "key_descriptors"),
            ("before", "before_descriptors"),
            ("after", "after_descriptors"),
        ):
            typed = getattr(event, f"typed_{image_name}", None)
            image = getattr(event, image_name, None)
            if image is None:
                continue
            new_image: dict[str, Any] = {}
            new_descriptors: dict[str, SourceTypeDescriptor] = {}
            new_typed_fields: dict[str, FieldValue] = {}
            for raw_name, value in image.items():
                name = naming.normalize(str(raw_name))
                descriptor = descriptors.get(name)
                if descriptor is None:
                    descriptor = (getattr(event, descriptor_name, {}) or {}).get(raw_name)
                rule = self.policy.rule_for(table, name)
                if image_name == "key" and rule.action != "replicate":
                    raise self._refusal(
                        event,
                        name,
                        rule,
                        "a transformed or excluded source key cannot reconcile rows",
                    )
                typed_field = typed.field(name) if typed is not None else FieldValue.absent()
                if typed_field.state is FieldState.UNCHANGED_TOAST:
                    if rule.action != "replicate":
                        raise self._refusal(
                            event,
                            name,
                            rule,
                            "an unchanged TOAST field has no safe policy value",
                        )
                    new_image[name] = value
                    if descriptor is not None:
                        new_descriptors[name] = descriptor
                    new_typed_fields[name] = FieldValue.unchanged_toast(descriptor)
                    continue
                if rule.action == "exclude":
                    if image_name == "key":
                        raise self._refusal(event, name, rule, "an excluded source key cannot reconcile rows")
                    if rule.rule_id == "__unmatched__" and self.policy.enabled:
                        event.policy_alerts.append(
                            {
                                "source_schema": event.schema,
                                "source_table": event.table,
                                "target_table": event.qualified_table,
                                "column": name,
                                "action": "exclude",
                                "rule_id": rule.rule_id,
                                "policy_epoch": self.policy.epoch,
                                "policy_digest": self.policy.digest,
                                "event_id": None,
                                "source_lsn": event.lsn,
                                "code": "unmatched_column_excluded",
                            }
                        )
                    continue
                if descriptor is None and rule.action != "mask":
                    raise self._refusal(event, name, rule, "the column has no catalog descriptor")
                try:
                    transformed, transformed_descriptor = self.policy.transform(
                        value,
                        descriptor,
                        rule,
                        output_text=_source_output_for(event, image_name, name),
                    ) if rule.action in {"mask", "hash", "truncate"} else (value, descriptor)
                except PolicyValueRefused as exc:
                    # PostgreSQL money and xml are deliberately represented as
                    # VARCHAR downstream.  If the connector did not carry an
                    # authoritative OUTPUT proof, omit that sensitive value and
                    # emit only a value-free policy alert; neither type may block
                    # a whole table.  Keys still fail above because omission would
                    # make reconciliation impossible.
                    if (
                        descriptor is not None
                        and str(descriptor.kind).lower() in {"money", "xml"}
                        and image_name != "key"
                    ):
                        event.policy_alerts.append(
                            {
                                "source_schema": event.schema,
                                "source_table": event.table,
                                "target_table": event.qualified_table,
                                "column": name,
                                "action": rule.action,
                                "rule_id": rule.rule_id,
                                "policy_epoch": self.policy.epoch,
                                "policy_digest": self.policy.digest,
                                "event_id": None,
                                "source_lsn": event.lsn,
                                "code": "money_xml_output_proof_unavailable",
                            }
                        )
                        continue
                    raise self._refusal(event, name, rule, str(exc)) from exc
                new_image[name] = transformed
                if transformed_descriptor is not None:
                    new_descriptors[name] = transformed_descriptor
                elif descriptor is not None:
                    new_descriptors[name] = descriptor
                if transformed_descriptor is not None:
                    new_typed_fields[name] = FieldValue.of(
                        transformed, transformed_descriptor
                    )
                elif typed_field.state is FieldState.EXPLICIT_NULL:
                    new_typed_fields[name] = FieldValue.explicit_null(descriptor)
                else:
                    new_typed_fields[name] = field_value(value, descriptor)
            # Preserve typed ABSENT/marker dispositions carried by a schema-enabled
            # envelope even when the legacy image has no corresponding mapping key.
            # They are protocol state, not values, and RowPatch must see them later.
            if typed is not None:
                for name, typed_field in typed.fields:
                    normalized = naming.normalize(name)
                    if normalized in new_typed_fields:
                        continue
                    rule = self.policy.rule_for(table, normalized)
                    if rule.action == "exclude":
                        continue
                    new_typed_fields[normalized] = typed_field
            setattr(event, image_name, new_image)
            setattr(event, descriptor_name, new_descriptors)
            setattr(
                event,
                f"typed_{image_name}",
                TypedImage(tuple(sorted(new_typed_fields.items()))),
            )

        # Output-function strings are sensitive source values too. They are
        # consumed only while transforming the copied image and must not remain on
        # the post-gate record, in assembler state, spill JSON, or diagnostics.
        event.output_texts = {}

        # A keyless DELETE is only safe when its full source before-image survived the
        # gate.  An excluded comparison field would make the physical match guesswork.
        if event.op == "d" and event.key is None and event.before is not None:
            catalog_names = set(descriptors)
            if catalog_names and not catalog_names.issubset(set(event.before)):
                raise self._refusal(event, "<row>", self.policy.rule_for(table, "row"), "keyless DELETE lacks a complete sanitized before-image")

        event.policy_epoch = int(self.policy.epoch)
        event.policy_digest = self.policy.digest
        event.sanitized = True
        event.delete_mode = getattr(event, "delete_mode", None)
        raw = getattr(event, "raw", None)
        if raw is not None and not isinstance(raw, AcknowledgementHandle):
            event.raw = AcknowledgementHandle(raw)
        return event

    def revalidate(self, event, descriptor_context=None):
        if not getattr(event, "sanitized", False):
            return self.sanitize(event, descriptor_context)
        if getattr(event, "policy_digest", None) != self.policy.digest:
            raise self._refusal(event, "<policy>", None, "record policy digest differs from active policy")
        if getattr(event, "output_texts", None):
            raise self._refusal(
                event,
                "<record>",
                None,
                "output-function proof survived the policy boundary",
            )
        raw = getattr(event, "raw", None)
        if raw is not None and not isinstance(raw, AcknowledgementHandle):
            raise self._refusal(
                event,
                "<record>",
                None,
                "decoded source mapping survived the policy boundary",
            )
        for image_name in ("key", "before", "after"):
            image = getattr(event, image_name, None) or {}
            typed = getattr(event, f"typed_{image_name}", None)
            for name in image:
                rule = self.policy.rule_for(event.qualified_table or "unknown.unknown", name)
                if rule.action == "exclude":
                    raise self._refusal(event, name, rule, "excluded field survived sanitization")
                if rule.action in {"mask", "hash", "truncate"}:
                    descriptor = (
                        (getattr(event, f"{image_name}_descriptors", {}) or {}).get(name)
                        or (typed.field(name).descriptor if typed is not None else None)
                    )
                    metadata = dict(getattr(descriptor, "metadata", ()) or ())
                    if metadata.get("policy_digest") != self.policy.digest:
                        raise self._refusal(
                            event,
                            name,
                            rule,
                            "transformed field lacks the active policy descriptor",
                        )
                    if typed is not None and typed.field(name).state is FieldState.UNCHANGED_TOAST:
                        raise self._refusal(
                            event,
                            name,
                            rule,
                            "transformed field retained an unchanged TOAST marker",
                        )
        return event

    def assert_sanitized(self, event) -> None:
        if not getattr(event, "sanitized", False):
            raise SchemaEvolutionRefused(
                "unsanitized record refused at the destination spill boundary",
                source_schema=getattr(event, "schema", None),
                source_table=getattr(event, "table", None),
                target=getattr(event, "qualified_table", None),
                refusal_origin="policy",
            )
        if getattr(event, "policy_digest", None) != self.policy.digest:
            raise SchemaEvolutionRefused(
                "record policy digest does not match the active policy",
                source_schema=getattr(event, "schema", None),
                source_table=getattr(event, "table", None),
                target=getattr(event, "qualified_table", None),
                refusal_origin="policy",
            )
        raw = getattr(event, "raw", None)
        if raw is not None and not isinstance(raw, AcknowledgementHandle):
            raise SchemaEvolutionRefused(
                "record retains a decoded source mapping after policy sanitization",
                source_schema=getattr(event, "schema", None),
                source_table=getattr(event, "table", None),
                target=getattr(event, "qualified_table", None),
                refusal_origin="policy",
            )

    @staticmethod
    def _refusal(event, column: str, rule: PIIRule | None, reason: str):
        action = rule.action if rule is not None else "policy"
        rule_id = rule.rule_id if rule is not None else "unknown"
        return SchemaEvolutionRefused(
            f"PII policy refused {action} for column {column!r} (rule {rule_id!r}): {reason}",
            source_schema=getattr(event, "schema", None),
            source_table=getattr(event, "table", None),
            target=getattr(event, "qualified_table", None),
            detected_lsn=getattr(event, "lsn", None),
            refusal_origin="policy",
        )


__all__ = [
    "POLICY_VERSION",
    "AcknowledgementHandle",
    "PIIPolicy",
    "PIIRule",
    "PolicyConfigurationError",
    "PolicyGate",
    "PolicyValueRefused",
    "PostgreSQLOutputText",
]
