# Architecture Decisions

## Why Medallion Architecture?

Bronze preserves raw data forever — if a Silver transformation
has a bug, we reprocess from Bronze without losing anything.
Silver is the single clean source of truth. Gold is purpose-built
for specific business questions — not a general query layer.

## Why PySpark for Silver, SQL for Gold?

Silver transformations involve programmatic logic: filling nulls
conditionally, casting types, deriving new columns. PySpark DataFrame
API handles this more cleanly than SQL. Gold aggregations are pure
reporting logic — SQL is more readable, maintainable, and familiar
to analysts who may inherit this code.

## Why Delta Lake instead of Parquet?

Delta Lake adds ACID transactions, time travel, and schema enforcement
on top of Parquet files. Critically, Delta tables persist to the
Databricks metastore — making them accessible across notebooks and
sessions. Temp views created with createOrReplaceTempView() die when
the session ends. saveAsTable() persists permanently.

## Why flag incomplete records instead of dropping them?

Dropping rows with missing driver info loses revenue data. Flagging
with data_quality = 'incomplete' preserves all 500 trips in Silver
while allowing Gold queries to selectively exclude unreliable records
(e.g. driver performance table filters to data_quality = 'ok').

## Why append Bronze, overwrite Silver?

Bronze is append-only — new data adds to existing data, historical
records are never modified. Silver is always rebuilt as a clean view
of ALL Bronze data — overwrite ensures Silver stays consistent with
the current cleaning logic applied uniformly across all trips.