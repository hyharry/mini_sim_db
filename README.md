# mini_sim_db

Tiny CSV-backed simulation run tracker.

Use it either:
- locally via CLI (`sim_db.py`), or
- over REST (`sim_db_server.py` + `sim_db_client.py`) when you want a central DB host.

For full design notes, schema details, remote workflow, and examples, see **`tutorial.ipynb`**.

## Requirements

- Python 3

## Quick start (local CLI)

```bash
# initialize DB (default: ~/sim_db.csv)
python sim_db.py init

# add one case
python sim_db.py add \
  --case case_001 \
  --inp case_001.inp \
  --bin solver \
  --status start

# mark case done
python sim_db.py done --case case_001

# list all records
python sim_db.py list
```

Notes:
- allowed status values: `start`, `restart`, `done`
- if `--work-dir` is omitted, current working directory is stored

## Quick start (REST)

```bash
# terminal 1: start server
export SIM_DB_API_TOKEN='replace-me'
python sim_db_server.py --host 127.0.0.1 --port 8765 --db ~/sim_db.csv

# terminal 2: use client
export SIM_DB_API_TOKEN='replace-me'
python sim_db_client.py --url http://127.0.0.1:8765 health
python sim_db_client.py --url http://127.0.0.1:8765 init
python sim_db_client.py --url http://127.0.0.1:8765 create \
  --case c100 --inp c100.inp --bin solver --status start
python sim_db_client.py --url http://127.0.0.1:8765 done --case c100
python sim_db_client.py --url http://127.0.0.1:8765 list
```

## Tests

```bash
python -m unittest -v
```
