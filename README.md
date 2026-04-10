# mini_sim_db

Tiny simulation run tracker.

Now SQLite-backed (`sqlite3` stdlib) with the same CLI + REST workflow.

- If you pass a `.csv` path (old usage), it is treated as compatibility input and mapped to a sibling `.sqlite3` DB file.
- If that CSV exists and the SQLite file does not, rows are auto-imported on first open.

## Quick start (local CLI)

```bash
# default argument stays compatible: ~/sim_db.csv
# actual DB file is ~/sim_db.sqlite3
python sim_db.py init

python sim_db.py add \
  --case case_001 \
  --inp case_001.inp \
  --bin solver \
  --status start

python sim_db.py done --case case_001

# easy inspection table
python sim_db.py list --table
python sim_db.py list --table --status done --limit 20

# optional explicit legacy import
python sim_db.py import-csv --csv ./legacy.csv
```

## Quick start (REST)

```bash
export SIM_DB_API_TOKEN='replace-me'
python sim_db_server.py --host 127.0.0.1 --port 8765 --db ~/sim_db.csv

python sim_db_client.py --url http://127.0.0.1:8765 --token "$SIM_DB_API_TOKEN" init
python sim_db_client.py --url http://127.0.0.1:8765 --token "$SIM_DB_API_TOKEN" create \
  --case c100 --inp c100.inp --bin solver --status start
python sim_db_client.py --url http://127.0.0.1:8765 --token "$SIM_DB_API_TOKEN" summary --status start --limit 20
```

## Tests

```bash
python -m unittest -v
```
