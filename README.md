A tiny simulation CSV database utility.

Useful for tracking simulation runs from submit scripts with a simple CLI.

## Requirements

- Python 3

## Revised behavior (v1.3)

- CSV DB tracks `work_dir` as a first-class field.
- Added `state_changed_at` timestamp (ISO date+time, local time) that updates:
  - when a case is added
  - when status changes via `done`
- Existing `created_at`/`updated_at` remain for compatibility.
- CSV column order is now stable and script-friendly:
  - `case, work_dir, bin, inp, input_files, status, note, notes, state_changed_at, created_at, updated_at`
- CLI supports both:
  - quick single-input usage via `--inp file.inp`
  - repeatable multi-input usage via `--input-file fileA --input-file fileB`
- `--inp` and `--input-file` can be used together. Stored fields:
  - `inp`: primary input (first one)
  - `input_files`: all inputs joined as `;`
- Optional short note field is supported via `--note` (legacy alias `--notes`).
- Status validation remains strict: only `start|restart|done`.
- Default DB path remains `~/sim_db.csv`.
- CRUD-style helper functions remain available in `sim_db.py` (`create_csv_db`, `add_cases`, `upd_cases`, `del_cases`, etc.).

## CLI

Default DB file: `~/sim_db.csv`

```bash
# initialize the default DB
python sim_db.py init

# use a custom DB file
python sim_db.py init --db /path/to/my_sim_db.csv
```

### Add a simulation item

Allowed status values are strictly:

- `start`
- `restart`
- `done`

Single-input convenience:

```bash
python sim_db.py add \
  --case case_001 \
  --work-dir "$PWD" \
  --inp model_001.inp \
  --bin solver_v2 \
  --status start
```

Multi-input (repeatable) usage:

```bash
python sim_db.py add \
  --case case_002 \
  --work-dir /scratch/project/case_002 \
  --input-file model_002.inp \
  --input-file mesh_002.inp \
  --bin solver_v2 \
  --status restart
```

With optional note and custom DB path:

```bash
python sim_db.py add \
  --case case_003 \
  --work-dir ./runs/case_003 \
  --inp model_003.inp \
  --input-file bc_003.inp \
  --bin solver_v2 \
  --status restart \
  --note "resubmission after mesh fix" \
  --db ./sim_db.csv
```

If `--work-dir` is not provided, the CLI stores the current working directory.

### Mark a case as done

```bash
python sim_db.py done --case case_001
```

This updates both `status=done` and `state_changed_at`.

### List DB content

```bash
python sim_db.py list
```

## Submit-script integration examples

Before pre-processing / submit:

```bash
#!/usr/bin/env bash
set -euo pipefail

CASE="case_123"
INP="${CASE}.inp"
BIN="solver_main"
DB="${DB:-$HOME/sim_db.csv}"

python /path/to/mini_sim_db/sim_db.py init --db "$DB"
python /path/to/mini_sim_db/sim_db.py add \
  --case "$CASE" \
  --work-dir "$PWD" \
  --inp "$INP" \
  --bin "$BIN" \
  --status start \
  --db "$DB"

# preproc and submit
./preproc "$INP"
./submit "$CASE"
```

After post-processing:

```bash
#!/usr/bin/env bash
set -euo pipefail

CASE="case_123"
DB="${DB:-$HOME/sim_db.csv}"
python /path/to/mini_sim_db/sim_db.py done --case "$CASE" --db "$DB"
```

## REST host + client (centralized writes)

New modules (without touching `sim_db.py`):

- `sim_db_server.py` — tiny stdlib HTTP JSON server
- `sim_db_client.py` — tiny stdlib client + CLI

Security defaults:

- Bearer token is mandatory (`--token` or `SIM_DB_API_TOKEN`)
- If no allowlist is configured, server only allows writes to the configured default DB path
- Optional write scope controls:
  - `--allowed-db-path /exact/path.csv`
  - `--allowed-base-dir /safe/base/dir`

Start server (local host, Linux/macOS):

```bash
export SIM_DB_API_TOKEN='replace-me'
python sim_db_server.py --host 127.0.0.1 --port 8765 --db ~/sim_db.csv
```

Start server (Windows PowerShell):

```powershell
$env:SIM_DB_API_TOKEN = 'replace-me'
python .\sim_db_server.py --host 127.0.0.1 --port 8765 --db $HOME\sim_db.csv
```

Client examples (Linux/macOS):

```bash
export SIM_DB_API_TOKEN='replace-me'
python sim_db_client.py --url http://127.0.0.1:8765 health
python sim_db_client.py --url http://127.0.0.1:8765 init
python sim_db_client.py --url http://127.0.0.1:8765 add \
  --case c100 --inp c100.inp --bin solver --status start --work-dir /work/c100
python sim_db_client.py --url http://127.0.0.1:8765 done --case c100
python sim_db_client.py --url http://127.0.0.1:8765 list
```

Client examples (Windows PowerShell):

```powershell
$env:SIM_DB_API_TOKEN = 'replace-me'
python .\sim_db_client.py --url http://127.0.0.1:8765 health
python .\sim_db_client.py --url http://127.0.0.1:8765 init
python .\sim_db_client.py --url http://127.0.0.1:8765 add `
  --case c100 --inp c100.inp --bin solver --status start --work-dir C:\work\c100
python .\sim_db_client.py --url http://127.0.0.1:8765 done --case c100
python .\sim_db_client.py --url http://127.0.0.1:8765 list
```

Windows notes:

- Server/client code now uses `pathlib.Path.resolve()` for path normalization and allowlist checks, so Windows drive-letter paths are handled more safely than plain string-prefix checks.
- If the server runs on a different Windows machine, bind to a reachable host/IP and open the firewall only for trusted sources.
- Keep the Bearer token in an environment variable rather than hardcoding it into scripts.

## Tests

```bash
python -m unittest -v
```

## CI/CD

This repository includes a GitHub Actions pipeline at `.github/workflows/ci-cd.yml`:

- **CI**: Runs unit tests (`python -m unittest -v`) on Python 3.10, 3.11, and 3.12 for every push and pull request.
- **CD**: On pushes to `main`, creates and uploads a source tarball artifact.
