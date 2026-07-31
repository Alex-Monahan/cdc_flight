# Real-time Data Replication with Debezium and Python

Saved copy of <https://dlthub.com/blog/debezium-and-dlt> (dlthub blog, author **Ismail Simsek**),
captured 2026-07-30. It is a dlt-flavoured rewrite of the original Debezium post
<https://debezium.io/blog/2025/02/01/real-time-data-replication-with-debezium-and-python/>.
Code in the post is Apache-2.0 (from `memiiso/pydbzengine`).

---

## Premise

> Change Data Capture (CDC) is the gold standard

for replicating operational data for analytics: scalable, near real-time, captures every
modification.

## Stack

1. **Debezium** — reads the database transaction log, produces change events.
2. **pydbzengine** — Python wrapper (JPype) around the Debezium *embedded* engine. No Kafka.
3. **dlt** — extract/load into a destination.
4. **DuckDB** — the destination in the example.
5. **Testcontainers** — throwaway Postgres for the demo.

## The example (verbatim structure)

### Environment setup

```python
import os
from pathlib import Path
import dlt
import duckdb
from testcontainers.core.config import testcontainers_config
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer
from pydbzengine import DebeziumJsonEngine, Properties
from pydbzengine.debeziumdlt import DltChangeHandler
from pydbzengine.helper import Utils

CURRENT_DIR = Path(__file__).parent
DUCKDB_FILE = CURRENT_DIR.joinpath("dbz_cdc_events_example.duckdb")
OFFSET_FILE = CURRENT_DIR.joinpath('postgresql-offsets.dat')

if OFFSET_FILE.exists():
    os.remove(OFFSET_FILE)
if DUCKDB_FILE.exists():
    os.remove(DUCKDB_FILE)

def wait_for_postgresql_to_start(self) -> None:
    wait_for_logs(self, ".*database system is ready to accept connections.*")
    wait_for_logs(self, ".*PostgreSQL init process complete.*")

class DbPostgresql:
    POSTGRES_USER = "postgres"
    POSTGRES_PASSWORD = "postgres"
    POSTGRES_DBNAME = "postgres"
    POSTGRES_IMAGE = "debezium/example-postgres:3.0.0.Final"
    POSTGRES_HOST = "localhost"
    POSTGRES_PORT_DEFAULT = 5432
    CONTAINER: PostgresContainer = (PostgresContainer(image=POSTGRES_IMAGE,
                                                      port=POSTGRES_PORT_DEFAULT,
                                                      username=POSTGRES_USER,
                                                      password=POSTGRES_PASSWORD,
                                                      dbname=POSTGRES_DBNAME,
                                                      )
                                    .with_exposed_ports(POSTGRES_PORT_DEFAULT)
                                    )
    PostgresContainer._connect = wait_for_postgresql_to_start

    def start(self):
        testcontainers_config.ryuk_disabled = True
        print("Starting Postgresql Db...")
        self.CONTAINER.start()

    def stop(self):
        print("Stopping Postgresql Db...")
        self.CONTAINER.stop()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
```

### Debezium configuration

```python
def debezium_engine_props(sourcedb: DbPostgresql):
    props = Properties()
    props.setProperty("name", "engine")
    props.setProperty("snapshot.mode", "initial_only")
    props.setProperty("database.hostname", sourcedb.CONTAINER.get_container_host_ip())
    props.setProperty("database.port",
                      sourcedb.CONTAINER.get_exposed_port(sourcedb.POSTGRES_PORT_DEFAULT))
    props.setProperty("database.user", sourcedb.POSTGRES_USER)
    props.setProperty("database.password", sourcedb.POSTGRES_PASSWORD)
    props.setProperty("database.dbname", sourcedb.POSTGRES_DBNAME)
    props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
    props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
    props.setProperty("offset.storage.file.filename", OFFSET_FILE.as_posix())
    props.setProperty("max.batch.size", "5")
    props.setProperty("poll.interval.ms", "10000")
    props.setProperty("converter.schemas.enable", "false")
    props.setProperty("offset.flush.interval.ms", "1000")
    props.setProperty("database.server.name", "testc")
    props.setProperty("database.server.id", "1234")
    props.setProperty("topic.prefix", "testc")
    props.setProperty("schema.whitelist", "inventory")
    props.setProperty("database.whitelist", "inventory")
    props.setProperty("table.whitelist", "inventory.*")
    props.setProperty("replica.identity.autoset.values", "inventory.*:FULL")
    # debezium unwrap message
    props.setProperty("transforms", "unwrap")
    props.setProperty("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
    props.setProperty("transforms.unwrap.add.fields", "op,table,source.ts_ms,sourcedb,ts_ms")
    props.setProperty("transforms.unwrap.delete.handling.mode", "rewrite")
    return props
```

### Custom handlers

```python
from pydbzengine import BasePythonChangeHandler, ChangeEvent

class MyXYZChangeHandler(BasePythonChangeHandler):
    def handleJsonBatch(self, records: List[ChangeEvent]):
        # Process your data here!
        for record in records:
            ...
```

### Main

```python
def main():
    sourcedb = DbPostgresql()
    sourcedb.start()

    props = debezium_engine_props(sourcedb=sourcedb)

    dlt_pipeline = dlt.pipeline(
        pipeline_name="dbz_cdc_events_example",
        destination="duckdb",
        dataset_name="dbz_data"
    )

    handler = DltChangeHandler(dlt_pipeline=dlt_pipeline)
    engine = DebeziumJsonEngine(properties=props, handler=handler)

    Utils.run_engine_async(engine=engine, timeout_sec=60)
    # engine.run()  # synchronous, no timeout

if __name__ == "__main__":
    main()
```

### Querying the result

```python
con = duckdb.connect(DUCKDB_FILE.as_posix())
result = con.sql("SHOW ALL TABLES").fetchall()
for r in result:
    database, schema, table = r[:3]
    if schema == "dbz_data":
        print(f"Data in table {table}:")
        con.sql(f"select * from {database}.{schema}.{table} limit 5").show()
```

### Getting started

```bash
pip install pydbzengine[dev]
python dlt_consuming.py
```

Full example: <https://github.com/memiiso/pydbzengine/blob/main/pydbzengine/examples/dlt_consuming.py>

## Claim

> a powerful and simple solution for CDC scenarios, enabling real-time data synchronization
> and analysis

`DltChangeHandler` keeps separation of concerns while handling the dlt integration and
data loading.

## The handler the post relies on (upstream source, `pydbzengine/handlers/dlt.py`)

```python
@dlt.source
def debezium_source_events(records: list[ChangeEvent]):
    table_events: dict[str, list[str]] = {}
    for e in records:
        if e.value() is None or not str(e.value()).strip():
            continue
        table = str(e.destination()).replace(".", "_")
        val = json.loads(str(e.value()))
        if table in table_events:
            table_events[table].append(val)
        else:
            table_events[table] = [val]

    for table_name, events in table_events.items():
        yield dlt.resource(events, name=table_name)


class DltChangeHandler(BasePythonChangeHandler):
    def __init__(self, dlt_pipeline):
        self.dlt_pipeline = dlt_pipeline
        self.log = logging.getLogger(self.LOGGER_NAME)

    def handleJsonBatch(self, records: list[ChangeEvent]):
        self.log.info(f"Received {len(records)} records")
        self.dlt_pipeline.run(debezium_source_events(records))
        self.log.info(f"Consumed {len(records)} records")
```

## Links from the post

* Original Debezium post: <https://debezium.io/blog/2025/02/01/real-time-data-replication-with-debezium-and-python/>
* pydbzengine: <https://github.com/memiiso/pydbzengine> (Apache-2.0)
