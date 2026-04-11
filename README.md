# mini_sim_db

Tiny local-first simulation run tracker.

Core storage is SQLite (`sim_db.py`). Remote host/client transport now lives in `remote_api/` as an optional module.

- `.csv` paths are still accepted for compatibility and map to a sibling `.sqlite3` DB.
- If legacy CSV exists and SQLite does not, rows are auto-imported on first open.
- `note` is the canonical free-text field. `notes` is accepted only as a backward-compatible CLI/input alias.
- `updated_at` is the canonical change timestamp exposed by the CLI and list output.

## Quick start (local CLI)

```bash
./sim_db init

./sim_db add \
  --case case_001 \
  --inp case_001.inp \
  --bin solver \
  --status start

./sim_db done --job-id <job_id>
./sim_db list --table
```

`sim_db.py` is directly executable too, so both of these work:

```bash
./sim_db list --table
./sim_db.py list --table
```

## CLI behavior notes

### 1) `case` vs `job_id`

- `case` is the human-chosen primary key in the database and must be unique.
- `job_id` is a stable derived identifier based on:
  - `case`
  - `work_dir`
  - `inp`
  - `input_files`

Use `--job-id` for state changes when you want the safest, most explicit target.

### 2) `note` / `notes`

`note` is the canonical field now.

- `--note` is the preferred CLI flag
- `--notes` still works as a deprecated alias
- list output and in-memory table views expose `note`

### 3) timestamps

`updated_at` is the canonical state/change timestamp exposed by the CLI.
`state_changed_at` is treated as legacy/internal compatibility data and is no longer shown in normal list output.

## Helpful CLI examples

```bash
# top-level help
./sim_db --help

# detailed subcommand help with examples
./sim_db add --help
./sim_db done --help

# add a multi-input job
./sim_db add \
  --case wing_load_01 \
  --bin solver \
  --input-file mesh.inp \
  --input-file load.inp \
  --status start \
  --note "baseline run"

# list jobs in a compact table
./sim_db list --table

# mark a job as done by explicit identifier
./sim_db done --job-id 0123abcd4567ef89
```

## Local-first sync workflow (JSON artifact)

```bash
# inspect unsynced local updates
./sim_db sync-status --table

# export pending rows to a portable artifact
./sim_db sync-export --out ./sync-out.json

# import artifact from another machine
./sim_db sync-import --in ./sync-out.json
```

Sync format: JSON `mini_sim_db_sync_v1` with full row snapshots. Merge policy: per `job_id`, newer `updated_at` wins; if local is newer, import reports a conflict for manual review.

## Optional REST transport

```bash
export SIM_DB_API_TOKEN='replace-me'
python remote_api/server.py --host 127.0.0.1 --port 8765 --db ~/sim_db.csv
python remote_api/client.py --url http://127.0.0.1:8765 --token "$SIM_DB_API_TOKEN" health
```

Compatibility entrypoints are kept (`sim_db_server.py`, `sim_db_client.py`).

## Tests

```bash
python3 -m unittest -v
```
