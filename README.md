A tiny simulation CSV database utility.

Useful for tracking simulation runs from submit scripts with a simple CLI.

## Requirements

- Python 3

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

```bash
python sim_db.py add \
  --case case_001 \
  --inp model_001.inp \
  --bin solver_v2 \
  --status start
```

Optional notes and custom DB path:

```bash
python sim_db.py add \
  --case case_002 \
  --inp model_002.inp \
  --bin solver_v2 \
  --status restart \
  --notes "resubmission after mesh fix" \
  --db ./sim_db.csv
```

### Mark a case as done

```bash
python sim_db.py done --case case_001
```

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

python /path/to/mini_sim_db/sim_db.py init
python /path/to/mini_sim_db/sim_db.py add \
  --case "$CASE" \
  --inp "$INP" \
  --bin "$BIN" \
  --status start

# preproc and submit
./preproc "$INP"
./submit "$CASE"
```

After post-processing:

```bash
#!/usr/bin/env bash
set -euo pipefail

CASE="case_123"
python /path/to/mini_sim_db/sim_db.py done --case "$CASE"
```

Using a project-local DB instead of `~/sim_db.csv`:

```bash
DB="$(pwd)/sim_db.csv"
python /path/to/mini_sim_db/sim_db.py init --db "$DB"
python /path/to/mini_sim_db/sim_db.py add --case case_x --inp case_x.inp --bin solver --status start --db "$DB"
python /path/to/mini_sim_db/sim_db.py done --case case_x --db "$DB"
```

## Tests

```bash
python -m unittest -v
```

## CI/CD

This repository includes a GitHub Actions pipeline at `.github/workflows/ci-cd.yml`:

- **CI**: Runs unit tests (`python -m unittest -v`) on Python 3.10, 3.11, and 3.12 for every push and pull request.
- **CD**: On pushes to `main`, creates and uploads a source tarball artifact.
