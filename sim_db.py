from __future__ import annotations

"""
simple database for simulations and more (SQLite-backed CRUD)

author: hyharry@github
license: MIT License
version: 2.0
"""

__doc__ = 'simple database for simulations and more (SQLite-backed CRUD)'

import argparse
import csv
import hashlib
import json
import os
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

ALLOWED_STATUS = {'start', 'restart', 'done'}
DEFAULT_DB_PATH = os.path.expanduser('~/sim_db.csv')  # kept for CLI compatibility
JOB_ID_FIELD = 'job_id'
CLI_FIELDS = [
    'work_dir',
    'bin',
    'inp',
    'input_files',
    JOB_ID_FIELD,
    'extra_params',
    'status',
    'note',
    'notes',
    'state_changed_at',
    'created_at',
    'updated_at',
    'run_host',
]
PREFERRED_FIELD_ORDER = ['case', *CLI_FIELDS]


def _now_iso() -> str:
    return datetime.now().isoformat(timespec='milliseconds')


def _ordered_fieldnames(fieldnames: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for field in fieldnames:
        if field and field not in seen:
            seen.add(field)
            unique.append(field)

    ordered: list[str] = []
    for field in PREFERRED_FIELD_ORDER:
        if field in seen:
            ordered.append(field)

    extras = sorted(f for f in unique if f not in set(PREFERRED_FIELD_ORDER))
    return [*ordered, *extras]


def _serialize_input_files(input_files: list[str]) -> str:
    return ';'.join(input_files)


def _parse_input_files(value: str | None) -> list[str]:
    if not value:
        return []
    return [part for part in str(value).split(';') if part]


def derive_job_id(
    *,
    case: str,
    work_dir: str | None = None,
    inp: str | None = None,
    input_files: list[str] | None = None,
) -> str:
    payload: dict[str, Any] = {'case': str(case)}
    if work_dir:
        payload['work_dir'] = str(work_dir)
    if inp:
        payload['inp'] = str(inp)
    if input_files:
        payload['input_files'] = [str(path) for path in input_files if str(path)]

    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:16]


def _normalize_input_files(inp: str | None, input_files: list[str] | None) -> tuple[str, list[str]]:
    files: list[str] = []
    if inp:
        files.append(inp)
    if input_files:
        for f in input_files:
            if f and f not in files:
                files.append(f)

    if not files:
        raise ValueError("At least one input file is required (use --inp and/or --input-file)")

    return files[0], files


def _normalize_extra_params(extra_params: str | None, extra_param_pairs: list[str] | None) -> str:
    if extra_params and extra_param_pairs:
        raise ValueError("Use either --extra-params or --extra-param, not both")

    if extra_params is not None:
        return str(extra_params)

    out: dict[str, str] = {}
    for pair in extra_param_pairs or []:
        if "=" not in pair:
            raise ValueError(f"Invalid --extra-param '{pair}', expected key=value")
        key, value = pair.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid --extra-param '{pair}', empty key")
        out[key] = value

    if not out:
        return ''

    return json.dumps(out, sort_keys=True, ensure_ascii=False)


def _validate_status(status: str) -> None:
    if status not in ALLOWED_STATUS:
        allowed = ', '.join(sorted(ALLOWED_STATUS))
        raise ValueError(f"Invalid status '{status}'. Allowed: {allowed}")


def _db_paths(db_path: str) -> tuple[Path, Path | None]:
    requested = Path(db_path).expanduser()
    if requested.suffix.lower() == '.csv':
        sqlite_path = requested.with_suffix('.sqlite3')
        return sqlite_path, requested
    return requested, None


def _connect_db(db_path: str) -> tuple[sqlite3.Connection, str]:
    sqlite_path, csv_path = _db_paths(db_path)
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    first_create = not sqlite_path.exists()
    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row
    _ensure_schema(conn)
    if first_create and csv_path and csv_path.exists():
        _import_csv_into_conn(conn, csv_path)
    return conn, str(sqlite_path)


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sim_cases (
            "case" TEXT PRIMARY KEY,
            work_dir TEXT NOT NULL DEFAULT '',
            bin TEXT NOT NULL DEFAULT '',
            inp TEXT NOT NULL DEFAULT '',
            input_files TEXT NOT NULL DEFAULT '',
            job_id TEXT NOT NULL DEFAULT '',
            extra_params TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            note TEXT NOT NULL DEFAULT '',
            notes TEXT NOT NULL DEFAULT '',
            state_changed_at TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT '',
            run_host TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sim_cases_job_id ON sim_cases(job_id)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sim_case_extra (
            "case" TEXT NOT NULL,
            field TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY ("case", field),
            FOREIGN KEY("case") REFERENCES sim_cases("case") ON DELETE CASCADE
        )
        """
    )
    conn.commit()


def _import_csv_into_conn(conn: sqlite3.Connection, csv_path: Path) -> None:
    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            case = str(row.get('case', '')).strip()
            if not case:
                continue
            for k in PREFERRED_FIELD_ORDER:
                row.setdefault(k, '')
            row['notes'] = row.get('notes') or row.get('note') or ''
            row['note'] = row.get('note') or row.get('notes') or ''
            row['job_id'] = row.get('job_id') or derive_job_id(
                case=case,
                work_dir=row.get('work_dir') or None,
                inp=row.get('inp') or None,
                input_files=_parse_input_files(row.get('input_files')),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO sim_cases
                ("case", work_dir, bin, inp, input_files, job_id, extra_params, status, note, notes,
                 state_changed_at, created_at, updated_at, run_host)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    case,
                    str(row.get('work_dir', '')),
                    str(row.get('bin', '')),
                    str(row.get('inp', '')),
                    str(row.get('input_files', '')),
                    str(row.get('job_id', '')),
                    str(row.get('extra_params', '')),
                    str(row.get('status', '')),
                    str(row.get('note', '')),
                    str(row.get('notes', '')),
                    str(row.get('state_changed_at', '')),
                    str(row.get('created_at', '')),
                    str(row.get('updated_at', '')),
                    str(row.get('run_host', '')),
                ),
            )
            for key, value in row.items():
                if key in set(PREFERRED_FIELD_ORDER):
                    continue
                if key and value is not None and str(value) != '':
                    conn.execute(
                        'INSERT OR REPLACE INTO sim_case_extra("case", field, value) VALUES (?, ?, ?)',
                        (case, key, str(value)),
                    )
    conn.commit()


def _row_to_detail(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, str]:
    detail = {k: str(row[k] or '') for k in row.keys() if k != 'case'}
    extras = conn.execute(
        'SELECT field, value FROM sim_case_extra WHERE "case" = ? ORDER BY field',
        (row['case'],),
    ).fetchall()
    for ext in extras:
        detail[str(ext['field'])] = str(ext['value'])
    return detail


def _table_from_conn(conn: sqlite3.Connection) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    rows = conn.execute('SELECT * FROM sim_cases ORDER BY "case"').fetchall()
    for row in rows:
        out[str(row['case'])] = _row_to_detail(conn, row)
    return out


def _set_fields(conn: sqlite3.Connection, case: str, fields: Mapping[str, Any]) -> None:
    base_fields = {k: str(v) for k, v in fields.items() if k in set(CLI_FIELDS)}
    extras = {k: str(v) for k, v in fields.items() if k not in set(CLI_FIELDS) and k != 'case'}

    if base_fields:
        cols = sorted(base_fields.keys())
        set_sql = ', '.join([f'"{c}" = ?' for c in cols])
        vals = [base_fields[c] for c in cols]
        conn.execute(f'UPDATE sim_cases SET {set_sql} WHERE "case" = ?', (*vals, case))

    for key, value in extras.items():
        conn.execute(
            'INSERT OR REPLACE INTO sim_case_extra("case", field, value) VALUES (?, ?, ?)',
            (case, key, value),
        )


def _ensure_case_exists(conn: sqlite3.Connection, case: str, db_path: str) -> None:
    row = conn.execute('SELECT "case" FROM sim_cases WHERE "case" = ?', (case,)).fetchone()
    if row is None:
        raise ValueError(f"Case '{case}' not found in {db_path}")


def _read_sim_db(db_path: str) -> tuple[list[str], list[dict[str, str]]]:
    conn, sqlite_path = _connect_db(db_path)
    try:
        table = _table_from_conn(conn)
        fieldnames = _ordered_fieldnames(['case', *{k for v in table.values() for k in v.keys()}])
        rows = [{'case': case, **detail} for case, detail in table.items()]
        return fieldnames, rows
    finally:
        conn.close()


def create_csv_db(fn_csv: str, dic: Mapping[str, Mapping[str, Any]]) -> None:
    """Backward-compatible API name; creates SQLite DB from mapping."""
    sqlite_path, _ = _db_paths(fn_csv)
    if sqlite_path.exists():
        raise Exception(f'{sqlite_path} already created, you can add items!')

    conn, sqlite_path_str = _connect_db(fn_csv)
    try:
        for case, detail in dic.items():
            now = _now_iso()
            values = {
                'work_dir': str(detail.get('work_dir', detail.get('directory', ''))),
                'bin': str(detail.get('bin', detail.get('exec_bin', ''))),
                'inp': str(detail.get('inp', '')),
                'input_files': _serialize_input_files(detail.get('input_files', []) if isinstance(detail.get('input_files'), list) else []),
                'status': str(detail.get('status', '')),
                'note': str(detail.get('note', detail.get('notes', ''))),
                'notes': str(detail.get('notes', detail.get('note', ''))),
                'state_changed_at': str(detail.get('state_changed_at', now)),
                'created_at': str(detail.get('created_at', now)),
                'updated_at': str(detail.get('updated_at', now)),
                'extra_params': str(detail.get('extra_params', '')),
                'run_host': str(detail.get('run_host', '')),
            }
            values['job_id'] = str(detail.get('job_id') or derive_job_id(case=case, work_dir=values['work_dir'] or None, inp=values['inp'] or None, input_files=_parse_input_files(values['input_files'])))
            conn.execute(
                """
                INSERT INTO sim_cases("case", work_dir, bin, inp, input_files, job_id, extra_params, status,
                                      note, notes, state_changed_at, created_at, updated_at, run_host)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    case,
                    values['work_dir'],
                    values['bin'],
                    values['inp'],
                    values['input_files'],
                    values['job_id'],
                    values['extra_params'],
                    values['status'],
                    values['note'],
                    values['notes'],
                    values['state_changed_at'],
                    values['created_at'],
                    values['updated_at'],
                    values['run_host'],
                ),
            )
            for k, v in detail.items():
                if k in {'case', 'directory', 'exec_bin', *CLI_FIELDS}:
                    continue
                conn.execute(
                    'INSERT OR REPLACE INTO sim_case_extra("case", field, value) VALUES (?, ?, ?)',
                    (case, str(k), str(v)),
                )
        conn.commit()
    finally:
        conn.close()
    print(f'mini sim database: {sqlite_path_str}, created! CREATE table')


def add_cases(fn_csv: str, sim_cases: Mapping[str, Mapping[str, Any]]) -> None:
    conn, sqlite_path = _connect_db(fn_csv)
    try:
        for case, detail in sim_cases.items():
            if conn.execute('SELECT 1 FROM sim_cases WHERE "case" = ?', (case,)).fetchone():
                print(f'{case} already in db (key), skip')
                continue
            payload = {k: str(v) for k, v in detail.items()}
            now = _now_iso()
            conn.execute(
                """
                INSERT INTO sim_cases("case", work_dir, bin, inp, input_files, job_id, extra_params, status,
                                      note, notes, state_changed_at, created_at, updated_at, run_host)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    case,
                    payload.get('work_dir', ''),
                    payload.get('bin', ''),
                    payload.get('inp', ''),
                    payload.get('input_files', ''),
                    payload.get('job_id') or derive_job_id(case=case, work_dir=payload.get('work_dir') or None, inp=payload.get('inp') or None, input_files=_parse_input_files(payload.get('input_files'))),
                    payload.get('extra_params', ''),
                    payload.get('status', ''),
                    payload.get('note', payload.get('notes', '')),
                    payload.get('notes', payload.get('note', '')),
                    payload.get('state_changed_at', now),
                    payload.get('created_at', now),
                    payload.get('updated_at', now),
                    payload.get('run_host', ''),
                ),
            )
            for key, value in payload.items():
                if key not in set(CLI_FIELDS):
                    conn.execute(
                        'INSERT OR REPLACE INTO sim_case_extra("case", field, value) VALUES (?, ?, ?)',
                        (case, key, value),
                    )
        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM sim_cases").fetchone()[0]
    finally:
        conn.close()
    print(f'mini sim database: {sqlite_path}, updated! INSERT {len(sim_cases)} items, now total {total} items')


def add_case_info(fn_csv: str, new_info: str, case_val_d: Mapping[str, Any]) -> None:
    conn, _ = _connect_db(fn_csv)
    try:
        for case, value in case_val_d.items():
            _ensure_case_exists(conn, case, fn_csv)
            if new_info in set(CLI_FIELDS):
                conn.execute(f'UPDATE sim_cases SET "{new_info}" = ? WHERE "case" = ?', (str(value), case))
            else:
                conn.execute(
                    'INSERT OR REPLACE INTO sim_case_extra("case", field, value) VALUES (?, ?, ?)',
                    (case, new_info, str(value)),
                )
        conn.commit()
    finally:
        conn.close()
    print(f"new info '{new_info}' added!")


def upd_cases(fn_csv: str, sim_cases_new_info: Mapping[str, Mapping[str, Any]]) -> None:
    conn, sqlite_path = _connect_db(fn_csv)
    try:
        for case, detail in sim_cases_new_info.items():
            if not conn.execute('SELECT 1 FROM sim_cases WHERE "case" = ?', (case,)).fetchone():
                print(f'{case} not present in db (key), skip')
                continue
            _set_fields(conn, case, detail)
        conn.commit()
    finally:
        conn.close()
    print(f'mini sim database: {sqlite_path}, updated! UPDATE {len(sim_cases_new_info)} sim cases')


def del_cases(fn_csv: str, sim_case_list: list[str]) -> None:
    conn, sqlite_path = _connect_db(fn_csv)
    try:
        for case in sim_case_list:
            if conn.execute('SELECT 1 FROM sim_cases WHERE "case" = ?', (case,)).fetchone():
                print(f'{case} delete in db')
            else:
                print(f'{case} not present in db (key), skip')
        conn.executemany('DELETE FROM sim_cases WHERE "case" = ?', [(c,) for c in sim_case_list])
        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM sim_cases").fetchone()[0]
    finally:
        conn.close()
    print(f'mini sim database: {sqlite_path}, changed! DELETE {len(sim_case_list)} items, now total {total} items')


def list_case_info(fn_csv: str) -> list[str]:
    conn, _ = _connect_db(fn_csv)
    try:
        rows = conn.execute("SELECT DISTINCT field FROM sim_case_extra ORDER BY field").fetchall()
        cols = [c for c in CLI_FIELDS]
        cols.extend(str(r['field']) for r in rows)
    finally:
        conn.close()
    print(cols)
    return cols


def list_sim_db(fn_csv: str) -> dict[str, dict[str, str]]:
    conn, _ = _connect_db(fn_csv)
    try:
        table = _table_from_conn(conn)
    finally:
        conn.close()
    print(table)
    return table


def search_sim_db(fn_csv: str, col_condition: str) -> list[str]:
    conn, _ = _connect_db(fn_csv)
    try:
        m = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*==\s*'([^']*)'\s*$", col_condition)
        if not m:
            raise ValueError("Only simple conditions like status == 'DONE' are supported")

        col, wanted = m.groups()
        if col in set(CLI_FIELDS) or col == 'case':
            rows = conn.execute(f'SELECT "case" FROM sim_cases WHERE "{col}" = ? ORDER BY "case"', (wanted,)).fetchall()
            return [str(r['case']) for r in rows]
        rows = conn.execute(
            """
            SELECT "case" FROM sim_case_extra
            WHERE field = ? AND value = ?
            ORDER BY "case"
            """,
            (col, wanted),
        ).fetchall()
        return [str(r['case']) for r in rows]
    finally:
        conn.close()


def init_sim_db(db_path: str = DEFAULT_DB_PATH) -> None:
    db_path = os.path.expanduser(db_path)
    conn, sqlite_path = _connect_db(db_path)
    conn.close()
    print(f'Initialized database: {sqlite_path}')


def resolve_case_ref(rows: list[dict[str, str]], case_or_job_id: str) -> str:
    if any(row.get('case', '') == case_or_job_id for row in rows):
        return case_or_job_id

    matches: list[str] = []
    for row in rows:
        case = row.get('case', '')
        if not case:
            continue
        if row.get(JOB_ID_FIELD) == case_or_job_id:
            matches.append(case)

    if not matches:
        raise ValueError(f"case/job_id not found: {case_or_job_id}")
    if len(matches) > 1:
        joined = ', '.join(sorted(matches))
        raise ValueError(f"job_id matches multiple cases ({joined}), use case explicitly")
    return matches[0]


def add_sim_item(
    case: str,
    inp: str | None,
    bin_name: str,
    status: str,
    db_path: str = DEFAULT_DB_PATH,
    notes: str = '',
    input_files: list[str] | None = None,
    note: str | None = None,
    work_dir: str | None = None,
    extra_params: str | None = None,
) -> None:
    _validate_status(status)
    conn, sqlite_path = _connect_db(db_path)
    try:
        if conn.execute('SELECT 1 FROM sim_cases WHERE "case" = ?', (case,)).fetchone():
            raise ValueError(f"Case '{case}' already exists in {sqlite_path}")

        primary_inp, files = _normalize_input_files(inp, input_files)
        note_value = note if note is not None else notes
        now = _now_iso()
        resolved_work_dir = work_dir or os.getcwd()
        conn.execute(
            """
            INSERT INTO sim_cases("case", work_dir, bin, inp, input_files, job_id, extra_params, status,
                                  note, notes, state_changed_at, created_at, updated_at, run_host)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                case,
                resolved_work_dir,
                bin_name,
                primary_inp,
                _serialize_input_files(files),
                derive_job_id(case=case, work_dir=resolved_work_dir, inp=primary_inp, input_files=files),
                str(extra_params or ''),
                status,
                note_value,
                note_value,
                now,
                now,
                now,
                '',
            ),
        )
        conn.commit()
    finally:
        conn.close()
    print(f"Added case '{case}' with status '{status}'")


def mark_done(case: str, db_path: str = DEFAULT_DB_PATH) -> None:
    conn, sqlite_path = _connect_db(db_path)
    try:
        _ensure_case_exists(conn, case, sqlite_path)
        now = _now_iso()
        conn.execute(
            "UPDATE sim_cases SET status = 'done', state_changed_at = ?, updated_at = ? WHERE \"case\" = ?",
            (now, now, case),
        )
        conn.commit()
    finally:
        conn.close()
    print(f"Case '{case}' marked as done")


def list_items(db_path: str = DEFAULT_DB_PATH) -> dict[str, dict[str, str]]:
    conn, _ = _connect_db(db_path)
    try:
        return _table_from_conn(conn)
    finally:
        conn.close()


def list_view(
    db_path: str = DEFAULT_DB_PATH,
    status: str | None = None,
    run_host: str | None = None,
    sort_by: str = 'updated_at',
    desc: bool = True,
    limit: int | None = None,
) -> list[dict[str, str]]:
    rows = [{'case': case, **detail} for case, detail in list_items(db_path).items()]
    if status is not None:
        rows = [r for r in rows if r.get('status') == status]
    if run_host is not None:
        rows = [r for r in rows if r.get('run_host') == run_host]
    rows.sort(key=lambda x: x.get(sort_by, ''), reverse=desc)
    if limit is not None:
        rows = rows[: max(0, limit)]
    return rows


def _format_table(rows: list[dict[str, str]]) -> str:
    cols = ['case', 'status', 'job_id', 'bin', 'inp', 'updated_at', 'run_host', 'note']
    widths = {c: len(c) for c in cols}
    for row in rows:
        for c in cols:
            widths[c] = min(64, max(widths[c], len(str(row.get(c, '')))))

    def _trim(v: str, w: int) -> str:
        if len(v) <= w:
            return v
        return v[: max(0, w - 1)] + '…'

    sep = ' | '
    header = sep.join(c.ljust(widths[c]) for c in cols)
    line = '-+-'.join('-' * widths[c] for c in cols)
    body = [sep.join(_trim(str(row.get(c, '')), widths[c]).ljust(widths[c]) for c in cols) for row in rows]
    return '\n'.join([header, line, *body])


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Mini simulation SQLite DB CLI')
    sub = parser.add_subparsers(dest='command', required=True)

    p_init = sub.add_parser('init', help='Initialize DB file')
    p_init.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to DB (CSV path auto-maps to SQLite)')

    p_add = sub.add_parser('add', help='Add simulation item')
    p_add.add_argument('--case', required=True, help='Case name / unique key')
    p_add.add_argument('--inp', default=None, help='Primary input file (convenience for single-input cases)')
    p_add.add_argument('--input-file', action='append', default=[], help='Input file (repeatable)')
    p_add.add_argument('--bin', dest='bin_name', required=True, help='Executable / binary name')
    p_add.add_argument('--work-dir', '--wd', dest='work_dir', default=None, help='Working directory for this case (default: current dir)')
    p_add.add_argument('--extra-param', action='append', default=[], help='Extra runtime parameter key=value (repeatable)')
    p_add.add_argument('--extra-params', default=None, help='Raw extra runtime parameters string (for example JSON)')
    p_add.add_argument('--status', required=True, help='start|restart|done')
    p_add.add_argument('--note', default='', help='Optional short note/documentation text')
    p_add.add_argument('--notes', dest='note', help='Backward-compatible alias of --note')
    p_add.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to DB (CSV path auto-maps to SQLite)')

    p_done = sub.add_parser('done', help='Mark case status as done')
    done_target = p_done.add_mutually_exclusive_group(required=True)
    done_target.add_argument('--case', help='Case name / unique key')
    done_target.add_argument('--job-id', dest='job_id', help='Stable job identifier')
    p_done.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to DB (CSV path auto-maps to SQLite)')

    p_list = sub.add_parser('list', help='List simulation items')
    p_list.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to DB (CSV path auto-maps to SQLite)')
    p_list.add_argument('--status', default=None)
    p_list.add_argument('--run-host', default=None)
    p_list.add_argument('--sort-by', default='updated_at')
    p_list.add_argument('--asc', action='store_true')
    p_list.add_argument('--limit', type=int, default=None)
    p_list.add_argument('--table', action='store_true', help='Show compact table view (easy inspection)')

    p_import = sub.add_parser('import-csv', help='Import/merge rows from a legacy CSV file into SQLite DB')
    p_import.add_argument('--csv', required=True, help='Path to legacy CSV file')
    p_import.add_argument('--db', default=DEFAULT_DB_PATH, help='Target DB path (CSV path auto-maps to SQLite)')

    return parser


def import_csv(csv_path: str, db_path: str = DEFAULT_DB_PATH) -> int:
    conn, sqlite_path = _connect_db(db_path)
    try:
        before = conn.execute('SELECT COUNT(*) FROM sim_cases').fetchone()[0]
        _import_csv_into_conn(conn, Path(csv_path).expanduser())
        after = conn.execute('SELECT COUNT(*) FROM sim_cases').fetchone()[0]
    finally:
        conn.close()
    added = int(after) - int(before)
    print(f'Imported {added} rows from {os.path.expanduser(csv_path)} into {sqlite_path}')
    return max(0, added)


def main(argv: list[str] | None = None) -> int:
    parser = _build_cli()
    args = parser.parse_args(argv)

    try:
        if args.command == 'init':
            init_sim_db(args.db)
        elif args.command == 'add':
            add_sim_item(
                case=args.case,
                inp=args.inp,
                input_files=args.input_file,
                bin_name=args.bin_name,
                status=args.status,
                db_path=args.db,
                note=args.note,
                work_dir=args.work_dir,
                extra_params=_normalize_extra_params(args.extra_params, args.extra_param),
            )
        elif args.command == 'done':
            target_case = args.case
            if args.job_id:
                _, rows = _read_sim_db(args.db)
                target_case = resolve_case_ref(rows, args.job_id)
            mark_done(case=target_case, db_path=args.db)
        elif args.command == 'list':
            rows = list_view(
                db_path=args.db,
                status=args.status,
                run_host=args.run_host,
                sort_by=args.sort_by,
                desc=not args.asc,
                limit=args.limit,
            )
            if not rows:
                print('(empty)')
            elif args.table:
                print(_format_table(rows))
            else:
                for row in rows:
                    case = row.pop('case')
                    print(f'{case}: {row}')
        elif args.command == 'import-csv':
            import_csv(args.csv, args.db)
        else:
            parser.print_help()
            return 1
    except (FileNotFoundError, ValueError, sqlite3.Error) as exc:
        print(str(exc), file=sys.stderr)
        return 2

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
