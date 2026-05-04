# Oracle To PostgreSQL Sync Notes

Untuk versi yang lebih ramah operator/non-developer, baca
[Panduan Operator Awam](OPERATOR_QUICK_START_ID.md).

## 1. Environment

`.env` is loaded automatically. Manual `export` is not required.

Use the default file:

```bash
ops doctor --config config.yaml
```

Use another file:

```bash
ops --env-file .env.prod doctor --config config.yaml
```

Required values:

```dotenv
ORACLE_HOST=...
ORACLE_USER=...
ORACLE_PASSWORD=...

PG_HOST=...
PG_PORT=5432
PG_DATABASE=...
PG_USER=...
PG_PASSWORD=...
```

Optional Oracle values can be empty:

```dotenv
ORACLE_DSN=
ORACLE_PORT=1521
ORACLE_SERVICE_NAME=
ORACLE_SID=
ORACLE_SCHEMA=
ORACLE_CLIENT_LIB_DIR=
```

`ORACLE_DSN` is optional when `ORACLE_HOST`/`ORACLE_PORT` is used.

## 2. Precheck

```bash
ops doctor --config config.yaml
```

Good signs:

```text
env_loaded,OK
missing_env_vars,OK,none
resolved_pg_host,OK,<host>
resolved_oracle_host,OK,<host-or-dsn>
postgres_connection,OK,connected
oracle_connection,OK,connected
```

## 3. Audit One Table

```bash
ops audit --config config.yaml --tables A_HP_BATCH --exact-count
```

Open:

```text
reports/run_<timestamp>_<run_id>/report.html
reports/run_<timestamp>_<run_id>/report.xlsx
reports/run_<timestamp>_<run_id>/logs.txt
```

Check:

```text
source_schema
source_table
target_schema
target_table
effective_where
oracle_count_sql_summary
postgres_count_sql_summary
```

If the audit log does not show `where=...`, the audit is full-table. `where`
appears only when an effective filter is configured.

## 4. Sync Oracle To PostgreSQL

Dry run:

```bash
ops sync \
  --config config.yaml \
  --direction oracle-to-postgres \
  --tables A_HP_BATCH \
  --lob include \
  --mode truncate
```

Execute direct truncate:

```bash
ops sync \
  --config config.yaml \
  --direction oracle-to-postgres \
  --tables A_HP_BATCH \
  --lob include \
  --mode truncate \
  --go
```

Safer staging mode:

```bash
ops sync \
  --config config.yaml \
  --direction oracle-to-postgres \
  --tables A_HP_BATCH \
  --lob include \
  --mode truncate_safe \
  --go
```

## 5. Validate

Rowcount only:

```bash
ops sync --config config.yaml --tables A_HP_BATCH --rowcount-only
```

Missing/extra keys:

```bash
ops validate missing-keys --config config.yaml --tables A_HP_BATCH
```

Outputs:

```text
missing_keys_summary.csv
keys_in_oracle_not_in_postgres.csv
keys_in_postgres_not_in_oracle.csv
```

The key comparison scans the full ordered key stream. The CSV detail output is
sample-limited, but `MATCH`/`MISMATCH` is not based only on the first sample.

## 6. Troubleshooting

### Missing Env

Error:

```text
Environment variable PG_HOST is not set. Check .env or export it.
```

Check:

```bash
echo $PG_HOST
cat .env
```

`.env` format must be:

```dotenv
PG_HOST=my-host
```

Not:

```dotenv
PG_HOST = my-host
```

### ORACLE_DSN Is Not Set

`ORACLE_DSN` is optional. Either set it empty:

```dotenv
ORACLE_DSN=
```

or use the supported host form:

```dotenv
ORACLE_HOST=my-oracle-host
ORACLE_PORT=1521
ORACLE_SERVICE_NAME=my-service
```

The tool now also treats missing `ORACLE_DSN` as empty when `ORACLE_HOST` is used.

### Rowcount Mismatch

A sync cannot report `SUCCESS` when rowcounts differ.

Check:

```text
sync_result.csv
manifest.json
report.html
logs.txt
```

Important fields:

```text
rows_read_from_oracle
rows_written_to_postgres
rows_failed
oracle_row_count
postgres_row_count
row_count_match
row_count_diff
validation_status
```

### LOB Failure

Use:

```bash
--lob include
```

If a row fails, inspect:

```text
failed_row_samples
logs.txt
```

The sample includes table, chunk, row number, key values, column name, and error.

### Rollback

Safe modes register rollback metadata:

```bash
ops rollback <run_id> --config config.yaml
```

Prefer `truncate_safe` for production tables that need rollback protection.
