# mini_sim_db

Tiny local-first simulation run tracker.

Core storage is SQLite via `sim_db.py`.

## Essentials

- `job_id` is the real row identity / primary key
- `case` is a label and may appear on multiple rows
- local CLI workflow is the core path
- sync is supported for moving updates between DBs

## Quick start

```bash
./sim_db init

./sim_db add \
  --case wing_load \
  --inp wing_load.inp \
  --bin solver \
  --status start

./sim_db list --table
./sim_db done --job-id <job_id>
```

## Common commands

```bash
./sim_db add --help
./sim_db list --table
./sim_db find --text wing
./sim_db done --job-id <job_id>
./sim_db push /path/to/remote.sqlite3
./sim_db pull /path/to/remote.sqlite3
```

## Notes

- prefer `--job-id` for updates
- if multiple rows share the same `case`, `--case` updates can be ambiguous
- sync merge key is `job_id`
- newer `updated_at` wins during sync

## Tests

```bash
python3 -m unittest -v
```

## Full usage

See `tutorial.ipynb` for the fuller workflow and examples, including:
- repeated runs with the same `case`
- search usage
- pull/push sync usage
- artifact sync usage
- local browser view
- optional remote transport notes
