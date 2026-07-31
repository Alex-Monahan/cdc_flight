# Research forks

Upstream repos forked into the `Alex-Monahan` account and cloned here for reference
while building `cdc_flight`. All clones are `--depth 1` (we read them, we do not
develop in them). None of them is a dependency of the build except `pydbzengine`,
which `cdc_flight/pyproject.toml` installs straight from the **upstream** tag
`memiiso/pydbzengine@3.6.0.0` — the fork exists so we can pin/patch it later if
upstream moves or breaks.

| Local path | Fork | Upstream | Why it exists |
|---|---|---|---|
| `repos/pydbzengine` | https://github.com/Alex-Monahan/pydbzengine | https://github.com/memiiso/pydbzengine | **The blog's companion code.** The dlthub post is a rewrite of memiiso's Debezium blog post and links directly to `pydbzengine/examples/dlt_consuming.py`. It is the JPype bridge that runs the Debezium *embedded engine* in-process, and it vendors the Debezium jars (393 jars, Debezium **3.6.0.Final**). We read `pydbzengine/handlers/dlt.py` (the `DltChangeHandler` the blog uses), `helper.py` (`Utils.run_engine_async` / `run_engine_until_snapshot`), and `_jvm.py` (JVM bootstrap, classpath, `PythonChangeConsumer`). Also the place to look when we need to reach past the Python API into Debezium's `RecordCommitter` for exactly-once work. |
| `repos/dlt` | https://github.com/Alex-Monahan/dlt | https://github.com/dlt-hub/dlt | The loading half of the pipeline. Needed for: the `motherduck`/`duckdb` destination implementations (`dlt/destinations/impl/motherduck`), schema evolution + contract behaviour, naming conventions (which is how we discovered dlt strips leading underscores from `__op`), `merge`/`scd2` write dispositions we will need for rubric §1 and §8.2, and pipeline state handling for rubric §1.6/§3. |
| `repos/debezium` | https://github.com/Alex-Monahan/debezium | https://github.com/debezium/debezium | Reference only, shallow clone (~61 MB). We need the *Java* source when the Python layer is not enough: `debezium-connector-postgresql` for `offset.mismatch.strategy`-style slot handling (rubric 1.8), heartbeat + `heartbeat.action.query` (rubric 4.4–4.6), `ExtractNewRecordState` field names, TOAST `__debezium_unavailable_value` handling (2.6), incremental snapshot / signalling (rubric 3.3–3.7), and `decimal.handling.mode` / `time.precision.mode` type conversions (2.4). |

## Notes

* `pydbzengine` is **not on PyPI any more** (the vendored jars blow past the size limit).
  Install is `pip install "pydbzengine @ git+https://github.com/memiiso/pydbzengine.git@3.6.0.0"`.
  The clone here is ~400 MB on disk for that reason.
* `debezium/debezium` full history is enormous; `--depth 1` is intentional. If we ever need
  to bisect, fetch more depth on demand.
* The dlthub blog post itself does not have its own demo repo — it reuses
  `memiiso/pydbzengine`'s example. A saved copy of the post is in
  `cdc_flight/research/dlthub_debezium_and_dlt.md`.

## Refreshing

```bash
cd repos/<name>
git fetch upstream 2>/dev/null || gh repo sync Alex-Monahan/<name>
git pull --depth 1 origin main
```
