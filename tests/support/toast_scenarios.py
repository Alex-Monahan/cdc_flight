"""Reusable bounded 2.6 scenario values for unit and slow tests."""

from __future__ import annotations

import hashlib


def incompressible_text(size: int = 16_384) -> str:
    chunks = []
    counter = 0
    while sum(len(chunk) for chunk in chunks) < size:
        chunks.append(hashlib.sha256(f"toast-{counter}".encode()).hexdigest())
        counter += 1
    return "".join(chunks)[:size]


def body_digest(value: str | bytes) -> str:
    raw = value.encode() if isinstance(value, str) else value
    return hashlib.sha256(raw).hexdigest()
