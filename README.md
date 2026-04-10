# mini_sim_db

Tiny local-first simulation run tracker.

Core storage is SQLite (`sim_db.py`). Remote host/client transport now lives in `remote_api/` as an optional module.

- `.csv` paths are still accepted for compatibility and map to a sibling `.sqlite3` DB.
- If legacy CSV exists and SQLite does not, rows are auto-imported on first open.

## Quick start (local CLI)

```bash
python sim_db.py init

python sim_db.py add \
  --case case_001 \
  --inp case_001.inp \
  --bin solver \
  --status start

python sim_db.py done --case case_001
python sim_db.py list --table
```

## Local-first sync workflow (JSON artifact)

```bash
# inspect unsynced local updates
python sim_db.py sync-status --table

# export pending rows to a portable artifact
python sim_db.py sync-export --out ./sync-out.json

# import artifact from another machine
python sim_db.py sync-import --in ./sync-out.json
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
