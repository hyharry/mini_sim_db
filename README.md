# mini_sim_db

Tiny local-first simulation run tracker.

Core storage is SQLite (`sim_db.py`). Remote host/client transport lives in `remote_api/` as an optional module.

## Key model

- `job_id` is the real row identity and primary key.
- `case` is now a label and **may appear on multiple rows**.
- `job_id` is derived from `case + work_dir + inp + input_files`.
- `note` is the canonical text field.
- `updated_at` is the canonical change timestamp.

This means repeated runs of the same case are supported cleanly as long as the run-defining inputs differ.

## Quick start

```bash
./sim_db init

./sim_db add \
  --case wing_load \
  --inp wing_load.inp \
  --bin solver \
  --status start

./sim_db add \
  --case wing_load \
  --inp wing_load_variant.inp \
  --bin solver \
  --status restart

./sim_db list --table
./sim_db done --job-id <job_id>
```

## CLI behavior

### Add

Use `./sim_db add --help` for full help and examples.

### State changes

- prefer `--job-id`
- `--case` only works when it resolves to exactly one row
- if multiple rows share the same case label, the CLI tells you to use `--job-id`


## Search

A new `find` command is available for case-insensitive search.

Rules:
- bare text is treated like `*text*` automatically
- `*` works as a wildcard
- multiple filters combine with AND

Examples:

```bash
./sim_db find --text wing
./sim_db find --case wing --work-dir project_a
./sim_db find --input-file mesh --note baseline
```

`--text` searches across: `case`, `work_dir`, `inp`, `input_files`, `note`, and `bin`.

## Sync

```bash
./sim_db sync-status --table
./sim_db sync-export --out ./sync-out.json
./sim_db sync-import --in ./sync-out.json
```

Sync merge policy is per `job_id`: newer `updated_at` wins.

## Tests

```bash
python3 -m unittest -v
```
