#!/usr/bin/env python3
from __future__ import annotations

"""mini_sim_db: tiny local-first simulation run tracker."""

import argparse
import csv
import hashlib
import json
import os
import re
import socket
import sqlite3
import sys
import tempfile
import threading
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

ALLOWED_STATUS = {'start', 'restart', 'done'}
DEFAULT_DB_PATH = os.path.expanduser('~/sim_db.csv')
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
    for preferred in PREFERRED_FIELD_ORDER:
        if preferred in seen:
            ordered.append(preferred)
            seen.remove(preferred)
    ordered.extend(sorted(seen))
    return ordered


def _db_paths(db_path: str) -> tuple[Path, Path | None]:
    expanded = Path(db_path).expanduser()
    if expanded.suffix.lower() == '.csv':
        return expanded.with_suffix('.sqlite3'), expanded
    if expanded.suffix.lower() == '.sqlite3':
        return expanded, None
    return expanded.with_suffix('.sqlite3'), None


def _parse_input_files(raw: str | None) -> list[str]:
    if raw is None:
        return []
    parts = re.split(r'[;,]', str(raw))
    return [p.strip() for p in parts if p.strip()]


def _serialize_input_files(files: list[str]) -> str:
    return ';'.join([str(f).strip() for f in files if str(f).strip()])


def _normalize_input_files(inp: str | None, input_files: list[str] | None) -> tuple[str, list[str]]:
    files: list[str] = []
    if input_files:
        files.extend([str(f).strip() for f in input_files if str(f).strip()])
    if inp:
        inp_str = str(inp).strip()
        if inp_str and inp_str not in files:
            files.insert(0, inp_str)
    primary = files[0] if files else (str(inp).strip() if inp else '')
    return primary, files


def derive_job_id(case: str, work_dir: str | None = None, inp: str | None = None, input_files: list[str] | None = None) -> str:
    payload: dict[str, Any] = {'case': str(case)}
    if work_dir:
        payload['work_dir'] = str(work_dir)
    if inp:
        payload['inp'] = str(inp)
    files = [str(f) for f in (input_files or []) if str(f).strip()]
    if files:
        payload['input_files'] = files
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]


def _normalize_extra_params(raw: str | None, pairs: list[str] | None = None) -> str:
    items = [str(p).strip() for p in (pairs or []) if str(p).strip()]
    if raw and items:
        raise ValueError('Use either --extra-params or --extra-param, not both')
    if raw:
        return str(raw)
    if not items:
        return ''
    data: dict[str, str] = {}
    for item in items:
        if '=' not in item:
            raise ValueError(f"Invalid --extra-param '{item}', expected key=value")
        key, value = item.split('=', 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid --extra-param '{item}', empty key")
        data[key] = value.strip()
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _validate_status(status: str) -> str:
    normalized = str(status).strip().lower()
    if normalized not in ALLOWED_STATUS:
        choices = ', '.join(sorted(ALLOWED_STATUS))
        raise ValueError(f'Invalid status {status!r}. Allowed: {choices}')
    return normalized


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
        '''
        CREATE TABLE IF NOT EXISTS sim_cases (
            job_id TEXT PRIMARY KEY,
            "case" TEXT NOT NULL,
            work_dir TEXT NOT NULL DEFAULT '',
            bin TEXT NOT NULL DEFAULT '',
            inp TEXT NOT NULL DEFAULT '',
            input_files TEXT NOT NULL DEFAULT '',
            extra_params TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            note TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT '',
            run_host TEXT NOT NULL DEFAULT ''
        )
        '''
    )
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS sim_case_extra (
            job_id TEXT NOT NULL,
            field TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (job_id, field),
            FOREIGN KEY(job_id) REFERENCES sim_cases(job_id) ON DELETE CASCADE
        )
        '''
    )
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS sim_sync_state (
            job_id TEXT PRIMARY KEY,
            last_synced_updated_at TEXT NOT NULL DEFAULT '',
            last_exported_at TEXT NOT NULL DEFAULT '',
            last_imported_at TEXT NOT NULL DEFAULT ''
        )
        '''
    )
    conn.commit()


def _import_csv_into_conn(conn: sqlite3.Connection, csv_path: Path) -> None:
    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            case = str(row.get('case', '')).strip()
            if not case:
                continue
            note = str(row.get('note') or row.get('notes') or '')
            inp = str(row.get('inp', ''))
            input_files = str(row.get('input_files', ''))
            work_dir = str(row.get('work_dir', ''))
            job_id = str(row.get('job_id') or derive_job_id(case=case, work_dir=work_dir or None, inp=inp or None, input_files=_parse_input_files(input_files)))
            updated_at = str(row.get('updated_at') or row.get('state_changed_at') or row.get('created_at') or _now_iso())
            created_at = str(row.get('created_at') or updated_at)
            conn.execute(
                '''
                INSERT OR REPLACE INTO sim_cases(job_id, "case", work_dir, bin, inp, input_files, extra_params, status, note, created_at, updated_at, run_host)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    job_id,
                    case,
                    work_dir,
                    str(row.get('bin', '')),
                    inp,
                    input_files,
                    str(row.get('extra_params', '')),
                    str(row.get('status', '')),
                    note,
                    created_at,
                    updated_at,
                    str(row.get('run_host', '')),
                ),
            )
            for key, value in row.items():
                if key in {'case', 'job_id', 'work_dir', 'bin', 'inp', 'input_files', 'extra_params', 'status', 'note', 'notes', 'state_changed_at', 'created_at', 'updated_at', 'run_host'}:
                    continue
                if value is not None and str(value) != '':
                    conn.execute('INSERT OR REPLACE INTO sim_case_extra(job_id, field, value) VALUES (?, ?, ?)', (job_id, key, str(value)))
    conn.commit()


def _row_to_detail(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, str]:
    detail = {k: str(row[k] or '') for k in row.keys()}
    extras = conn.execute('SELECT field, value FROM sim_case_extra WHERE job_id = ? ORDER BY field', (row['job_id'],)).fetchall()
    for ext in extras:
        detail[str(ext['field'])] = str(ext['value'])
    return detail


def list_items(db_path: str = DEFAULT_DB_PATH) -> dict[str, dict[str, str]]:
    conn, _ = _connect_db(db_path)
    try:
        rows = conn.execute('SELECT * FROM sim_cases ORDER BY "case", created_at, job_id').fetchall()
        return {str(row['job_id']): _row_to_detail(conn, row) for row in rows}
    finally:
        conn.close()


def list_view(db_path: str = DEFAULT_DB_PATH, status: str | None = None, run_host: str | None = None, sort_by: str = 'updated_at', desc: bool = True, limit: int | None = None) -> list[dict[str, str]]:
    rows = list(list_items(db_path).values())
    if status is not None:
        rows = [r for r in rows if r.get('status') == status]
    if run_host is not None:
        rows = [r for r in rows if r.get('run_host') == run_host]
    rows.sort(key=lambda x: x.get(sort_by, ''), reverse=desc)
    if limit is not None:
        rows = rows[: max(0, limit)]
    return rows


def _wildcard_to_regex(pattern: str) -> str:
    escaped = re.escape(pattern)
    return '^' + escaped.replace('\*', '.*') + '$'


def _matches_pattern(value: str, pattern: str) -> bool:
    value = str(value or '')
    pattern = str(pattern or '').strip()
    if not pattern:
        return True
    if '*' not in pattern:
        pattern = f'*{pattern}*'
    return re.match(_wildcard_to_regex(pattern), value, flags=re.IGNORECASE) is not None


def find_items(
    db_path: str = DEFAULT_DB_PATH,
    text: str | None = None,
    case: str | None = None,
    work_dir: str | None = None,
    inp: str | None = None,
    input_file: str | None = None,
    note: str | None = None,
    bin_name: str | None = None,
    status: str | None = None,
    run_host: str | None = None,
    limit: int | None = None,
) -> list[dict[str, str]]:
    rows = list_view(db_path=db_path, status=None, run_host=None, sort_by='updated_at', desc=True, limit=None)
    out: list[dict[str, str]] = []
    for row in rows:
        haystacks = [row.get('case', ''), row.get('work_dir', ''), row.get('inp', ''), row.get('input_files', ''), row.get('note', ''), row.get('bin', '')]
        if text and not any(_matches_pattern(h, text) for h in haystacks):
            continue
        if case and not _matches_pattern(row.get('case', ''), case):
            continue
        if work_dir and not _matches_pattern(row.get('work_dir', ''), work_dir):
            continue
        if inp and not _matches_pattern(row.get('inp', ''), inp):
            continue
        if input_file and not _matches_pattern(row.get('input_files', ''), input_file):
            continue
        if note and not _matches_pattern(row.get('note', ''), note):
            continue
        if bin_name and not _matches_pattern(row.get('bin', ''), bin_name):
            continue
        if status and not _matches_pattern(row.get('status', ''), status):
            continue
        if run_host and not _matches_pattern(row.get('run_host', ''), run_host):
            continue
        out.append(row)
    if limit is not None:
        out = out[: max(0, limit)]
    return out


def list_sim_db(fn_csv: str) -> dict[str, dict[str, str]]:
    return list_items(fn_csv)


def _read_sim_db(db_path: str) -> tuple[list[str], list[dict[str, str]]]:
    table = list_items(db_path)
    fieldnames = _ordered_fieldnames([*{k for v in table.values() for k in v.keys()}])
    return fieldnames, list(table.values())


def resolve_job_id(rows: list[dict[str, str]], *, case: str | None = None, job_id: str | None = None) -> str:
    if job_id:
        if any(row.get('job_id') == job_id for row in rows):
            return job_id
        raise ValueError(f'job_id not found: {job_id}')
    if not case:
        raise ValueError('either case or job_id is required')
    matches = [row for row in rows if row.get('case') == case]
    if not matches:
        raise ValueError(f'case not found: {case}')
    if len(matches) > 1:
        ids = ', '.join(sorted(row.get('job_id', '') for row in matches))
        raise ValueError(f"case '{case}' matches multiple rows; use --job-id ({ids})")
    return str(matches[0]['job_id'])


def resolve_case_ref(rows: list[dict[str, str]], case_or_job_id: str) -> str:
    matches = [row for row in rows if row.get('case') == case_or_job_id]
    if len(matches) == 1:
        return str(matches[0]['case'])
    if len(matches) > 1:
        ids = ', '.join(sorted(row.get('job_id', '') for row in matches))
        raise ValueError(f"case '{case_or_job_id}' matches multiple rows; use job_id ({ids})")
    matches = [row for row in rows if row.get('job_id') == case_or_job_id]
    if not matches:
        raise ValueError(f'case/job_id not found: {case_or_job_id}')
    return str(matches[0]['case'])


def init_sim_db(db_path: str = DEFAULT_DB_PATH) -> None:
    conn, sqlite_path = _connect_db(db_path)
    conn.close()
    print(f'Initialized database: {sqlite_path}')


def add_sim_item(case: str, inp: str | None, bin_name: str, status: str, db_path: str = DEFAULT_DB_PATH, notes: str = '', input_files: list[str] | None = None, note: str | None = None, work_dir: str | None = None, extra_params: str | None = None) -> None:
    status = _validate_status(status)
    conn, _ = _connect_db(db_path)
    try:
        primary_inp, files = _normalize_input_files(inp, input_files)
        note_value = note if note is not None else notes
        now = _now_iso()
        resolved_work_dir = work_dir or os.getcwd()
        job_id = derive_job_id(case=case, work_dir=resolved_work_dir, inp=primary_inp, input_files=files)
        conn.execute(
            '''
            INSERT INTO sim_cases(job_id, "case", work_dir, bin, inp, input_files, extra_params, status, note, created_at, updated_at, run_host)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (job_id, case, resolved_work_dir, bin_name, primary_inp, _serialize_input_files(files), str(extra_params or ''), status, note_value, now, now, ''),
        )
        conn.commit()
    finally:
        conn.close()
    print(f"Added case '{case}' with status '{status}'")


def upd_case_by_job_id(db_path: str, job_id: str, fields: Mapping[str, Any]) -> None:
    conn, sqlite_path = _connect_db(db_path)
    try:
        row = conn.execute('SELECT job_id FROM sim_cases WHERE job_id = ?', (job_id,)).fetchone()
        if row is None:
            raise ValueError(f"job_id not present in db: {job_id}")
        mutable_base = {'work_dir', 'bin', 'inp', 'input_files', 'extra_params', 'status', 'note', 'created_at', 'updated_at', 'run_host'}
        base_fields = {k: str(v) for k, v in fields.items() if k in mutable_base}
        extra_fields = {k: str(v) for k, v in fields.items() if k not in mutable_base and k not in {'case', 'job_id', 'notes', 'state_changed_at'}}
        if 'notes' in fields and 'note' not in base_fields:
            base_fields['note'] = str(fields['notes'])
        if base_fields:
            cols = sorted(base_fields.keys())
            conn.execute('UPDATE sim_cases SET ' + ', '.join([f'"{c}" = ?' for c in cols]) + ' WHERE job_id = ?', (*[base_fields[c] for c in cols], job_id))
        for key, value in extra_fields.items():
            conn.execute('INSERT OR REPLACE INTO sim_case_extra(job_id, field, value) VALUES (?, ?, ?)', (job_id, key, value))
        conn.commit()
    finally:
        conn.close()
    print(f'mini sim database: {sqlite_path}, updated! UPDATE job {job_id}')


def upd_cases(fn_csv: str, sim_cases_new_info: Mapping[str, Mapping[str, Any]]) -> None:
    conn, sqlite_path = _connect_db(fn_csv)
    try:
        for case, detail in sim_cases_new_info.items():
            rows = conn.execute('SELECT job_id FROM sim_cases WHERE "case" = ? ORDER BY created_at, job_id', (case,)).fetchall()
            if len(rows) != 1:
                continue
            upd_case_by_job_id(fn_csv, str(rows[0]['job_id']), detail)
        conn.commit()
    finally:
        conn.close()
    print(f'mini sim database: {sqlite_path}, updated! UPDATE {len(sim_cases_new_info)} sim cases')


def mark_done(job_id: str | None = None, db_path: str = DEFAULT_DB_PATH, case: str | None = None) -> None:
    if job_id is None:
        _, rows = _read_sim_db(db_path)
        job_id = resolve_job_id(rows, case=case)
    now = _now_iso()
    upd_case_by_job_id(db_path, job_id, {'status': 'done', 'updated_at': now})
    print(f"Job '{job_id}' marked as done")


def del_case_by_job_id(db_path: str, job_id: str) -> None:
    conn, sqlite_path = _connect_db(db_path)
    try:
        conn.execute('DELETE FROM sim_cases WHERE job_id = ?', (job_id,))
        conn.execute('DELETE FROM sim_case_extra WHERE job_id = ?', (job_id,))
        conn.commit()
        total = conn.execute('SELECT COUNT(*) FROM sim_cases').fetchone()[0]
    finally:
        conn.close()
    print(f'mini sim database: {sqlite_path}, changed! DELETE 1 item, now total {total} items')


def del_cases(fn_csv: str, sim_case_list: list[str]) -> None:
    conn, sqlite_path = _connect_db(fn_csv)
    deleted = 0
    try:
        for case in sim_case_list:
            rows = conn.execute('SELECT job_id FROM sim_cases WHERE "case" = ?', (case,)).fetchall()
            for row in rows:
                conn.execute('DELETE FROM sim_case_extra WHERE job_id = ?', (str(row['job_id']),))
            conn.execute('DELETE FROM sim_cases WHERE "case" = ?', (case,))
            deleted += len(rows)
        conn.commit()
        total = conn.execute('SELECT COUNT(*) FROM sim_cases').fetchone()[0]
    finally:
        conn.close()
    print(f'mini sim database: {sqlite_path}, changed! DELETE {deleted} items, now total {total} items')


def create_csv_db(fn_csv: str, dic: Mapping[str, Mapping[str, Any]]) -> None:
    sqlite_path, _ = _db_paths(fn_csv)
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    if sqlite_path.exists():
        sqlite_path.unlink()
    conn = sqlite3.connect(str(sqlite_path))
    try:
        _ensure_schema(conn)
        now = _now_iso()
        for case, detail in dic.items():
            work_dir = str(detail.get('work_dir', detail.get('directory', '')))
            inp = str(detail.get('inp', ''))
            input_files = _serialize_input_files(detail.get('input_files') if isinstance(detail.get('input_files'), list) else _parse_input_files(detail.get('input_files')))
            job_id = str(detail.get('job_id') or derive_job_id(case=case, work_dir=work_dir or None, inp=inp or None, input_files=_parse_input_files(input_files)))
            conn.execute(
                '''
                INSERT INTO sim_cases(job_id, "case", work_dir, bin, inp, input_files, extra_params, status, note, created_at, updated_at, run_host)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (job_id, case, work_dir, str(detail.get('bin', detail.get('exec_bin', ''))), inp, input_files, str(detail.get('extra_params', '')), str(detail.get('status', '')), str(detail.get('note', detail.get('notes', ''))), str(detail.get('created_at', now)), str(detail.get('updated_at', detail.get('state_changed_at', now))), str(detail.get('run_host', ''))),
            )
            for k, v in detail.items():
                if k in {'case', 'directory', 'exec_bin', 'work_dir', 'bin', 'inp', 'input_files', 'job_id', 'extra_params', 'status', 'note', 'notes', 'created_at', 'updated_at', 'state_changed_at', 'run_host'}:
                    continue
                if v is not None and str(v) != '':
                    conn.execute('INSERT OR REPLACE INTO sim_case_extra(job_id, field, value) VALUES (?, ?, ?)', (job_id, str(k), str(v)))
        conn.commit()
    finally:
        conn.close()


def add_cases(fn_csv: str, sim_cases: Mapping[str, Mapping[str, Any]]) -> None:
    for case, detail in sim_cases.items():
        add_sim_item(
            case=case,
            inp=detail.get('inp'),
            input_files=detail.get('input_files') if isinstance(detail.get('input_files'), list) else _parse_input_files(detail.get('input_files')),
            bin_name=str(detail.get('bin', detail.get('exec_bin', ''))),
            status=str(detail.get('status', 'start')),
            db_path=fn_csv,
            note=str(detail.get('note', detail.get('notes', ''))),
            work_dir=str(detail.get('work_dir', detail.get('directory', '')) or ''),
            extra_params=str(detail.get('extra_params', '')),
        )


def add_case_info(fn_csv: str, new_info: str, case_val_d: Mapping[str, Any]) -> None:
    conn, _ = _connect_db(fn_csv)
    try:
        for case, value in case_val_d.items():
            rows = conn.execute('SELECT job_id FROM sim_cases WHERE "case" = ? ORDER BY created_at, job_id', (case,)).fetchall()
            if len(rows) != 1:
                raise ValueError(f"Case '{case}' not uniquely resolvable")
            upd_case_by_job_id(fn_csv, str(rows[0]['job_id']), {new_info: value})
    finally:
        conn.close()


def list_case_info(fn_csv: str) -> list[str]:
    conn, _ = _connect_db(fn_csv)
    try:
        rows = conn.execute('SELECT DISTINCT field FROM sim_case_extra ORDER BY field').fetchall()
        return [str(r['field']) for r in rows]
    finally:
        conn.close()


def search_sim_db(fn_csv: str, query: str) -> list[str]:
    m = re.match(r'\s*([A-Za-z_][A-Za-z0-9_]*)\s*==\s*[\'\"](.*)[\'\"]\s*$', query)
    if not m:
        raise ValueError("Only simple equality queries are supported, for example: owner == 'alice'")
    col, wanted = m.group(1), m.group(2)
    conn, _ = _connect_db(fn_csv)
    try:
        if col in set(CLI_FIELDS) or col == 'case':
            rows = conn.execute(f'SELECT DISTINCT "case" FROM sim_cases WHERE "{col}" = ? ORDER BY "case"', (wanted,)).fetchall()
            return [str(r['case']) for r in rows]
        rows = conn.execute(
            '''
            SELECT DISTINCT c."case"
            FROM sim_case_extra e
            JOIN sim_cases c ON c.job_id = e.job_id
            WHERE e.field = ? AND e.value = ?
            ORDER BY c."case"
            ''',
            (col, wanted),
        ).fetchall()
        return [str(r['case']) for r in rows]
    finally:
        conn.close()


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


def sync_status(db_path: str = DEFAULT_DB_PATH) -> dict[str, Any]:
    conn, _ = _connect_db(db_path)
    try:
        total = int(conn.execute('SELECT COUNT(*) FROM sim_cases').fetchone()[0])
        rows = conn.execute(
            '''
            SELECT c.* FROM sim_cases c
            LEFT JOIN sim_sync_state s ON c.job_id = s.job_id
            WHERE s.last_synced_updated_at IS NULL OR s.last_synced_updated_at < c.updated_at
            ORDER BY c.updated_at, c."case"
            '''
        ).fetchall()
        pending = [_row_to_detail(conn, row) for row in rows]
        synced = max(0, total - len(pending))
        last_export = conn.execute('SELECT MAX(last_exported_at) FROM sim_sync_state').fetchone()[0] or ''
        last_import = conn.execute('SELECT MAX(last_imported_at) FROM sim_sync_state').fetchone()[0] or ''
        return {'total_cases': total, 'pending_cases': len(pending), 'synced_cases': synced, 'last_exported_at': str(last_export), 'last_imported_at': str(last_import), 'pending': pending}
    finally:
        conn.close()


def sync_export(db_path: str, out_path: str, include_all: bool = False, mark_synced: bool = True) -> dict[str, Any]:
    conn, sqlite_path = _connect_db(db_path)
    exported_at = _now_iso()
    source_host = socket.gethostname()
    try:
        if include_all:
            rows = [_row_to_detail(conn, row) for row in conn.execute('SELECT * FROM sim_cases ORDER BY "case", created_at, job_id').fetchall()]
        else:
            rows = sync_status(db_path)['pending']
        artifact = {'format': 'mini_sim_db_sync_v1', 'exported_at': exported_at, 'source_host': source_host, 'source_db': sqlite_path, 'count': len(rows), 'items': rows}
        out_file = Path(out_path).expanduser()
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(json.dumps(artifact, indent=2, ensure_ascii=False, sort_keys=True) + '\n', encoding='utf-8')
        if mark_synced:
            for row in rows:
                job_id = str(row.get('job_id', ''))
                if job_id:
                    conn.execute('INSERT OR REPLACE INTO sim_sync_state(job_id, last_synced_updated_at, last_exported_at, last_imported_at) VALUES (?, ?, ?, COALESCE((SELECT last_imported_at FROM sim_sync_state WHERE job_id = ?), \"\"))', (job_id, str(row.get('updated_at', '')), exported_at, job_id))
            conn.commit()
        return {'ok': True, 'path': str(out_file), 'exported': len(rows), 'exported_at': exported_at}
    finally:
        conn.close()


def sync_import(db_path: str, in_path: str) -> dict[str, Any]:
    in_file = Path(in_path).expanduser()
    payload = json.loads(in_file.read_text(encoding='utf-8'))
    if payload.get('format') != 'mini_sim_db_sync_v1':
        raise ValueError('unsupported sync artifact format')
    items = payload.get('items')
    if not isinstance(items, list):
        raise ValueError('sync artifact must contain items list')
    imported_at = _now_iso()
    conn, _ = _connect_db(db_path)
    created = 0
    updated = 0
    skipped = 0
    conflicts: list[dict[str, str]] = []
    try:
        for raw in items:
            if not isinstance(raw, dict):
                continue
            job_id = str(raw.get('job_id', '')).strip()
            case = str(raw.get('case', '')).strip()
            if not job_id or not case:
                conflicts.append({'reason': 'missing_case_or_job_id', 'case': case, 'job_id': job_id})
                continue
            local = conn.execute('SELECT updated_at FROM sim_cases WHERE job_id = ?', (job_id,)).fetchone()
            incoming_updated = str(raw.get('updated_at', ''))
            if local is None:
                base = {k: str(v) for k, v in raw.items()}
                conn.execute(
                    '''INSERT INTO sim_cases(job_id, "case", work_dir, bin, inp, input_files, extra_params, status, note, created_at, updated_at, run_host)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        job_id, case, base.get('work_dir', ''), base.get('bin', ''), base.get('inp', ''), base.get('input_files', ''), base.get('extra_params', ''),
                        base.get('status', ''), base.get('note', ''), base.get('created_at', incoming_updated), incoming_updated, base.get('run_host', ''),
                    ),
                )
                created += 1
            else:
                local_updated = str(local['updated_at'] or '')
                if incoming_updated > local_updated:
                    upd_case_by_job_id(db_path, job_id, raw)
                    updated += 1
                elif incoming_updated == local_updated:
                    skipped += 1
                else:
                    conflicts.append({'reason': 'local_newer', 'case': case, 'job_id': job_id, 'local_updated_at': local_updated, 'incoming_updated_at': incoming_updated})
                    continue
            conn.execute('INSERT OR REPLACE INTO sim_sync_state(job_id, last_synced_updated_at, last_exported_at, last_imported_at) VALUES (?, ?, COALESCE((SELECT last_exported_at FROM sim_sync_state WHERE job_id = ?), \"\"), ?)', (job_id, incoming_updated, job_id, imported_at))
        conn.commit()
        return {'ok': True, 'imported_file': str(in_file), 'created': created, 'updated': updated, 'skipped': skipped, 'conflicts': conflicts}
    finally:
        conn.close()


def mark_start(job_id: str | None = None, db_path: str = DEFAULT_DB_PATH, case: str | None = None) -> None:
    if job_id is None:
        _, rows = _read_sim_db(db_path)
        job_id = resolve_job_id(rows, case=case)
    now = _now_iso()
    upd_case_by_job_id(db_path, job_id, {'status': 'start', 'updated_at': now})
    print(f"Job '{job_id}' marked as start")


def _view_payload(db_path: str) -> dict[str, Any]:
    rows = list_view(db_path=db_path, sort_by='updated_at', desc=True)
    columns = _ordered_fieldnames([*{k for row in rows for k in row.keys()}])
    return {'rows': rows, 'columns': columns}


def _view_html() -> str:
    return '''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>mini_sim_db view</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 16px; }
    .toolbar { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 10px; align-items: center; }
    input, select, button { padding: 6px 8px; }
    table { border-collapse: collapse; width: 100%; font-size: 13px; }
    th, td { border: 1px solid #ddd; padding: 6px; vertical-align: top; }
    th button { all: unset; cursor: pointer; color: #0a58ca; }
    tr:nth-child(even) { background: #fafafa; }
    .actions { display: flex; gap: 6px; }
    .muted { color: #666; font-size: 12px; }
  </style>
</head>
<body>
  <h2>mini_sim_db local view</h2>
  <div class="toolbar">
    <label>Filter <input id="filterText" placeholder="case / note / any column" /></label>
    <label>Sort by <select id="sortField"></select></label>
    <button id="ascBtn">Date/Field ↑</button>
    <button id="descBtn">Date/Field ↓</button>
    <button id="reloadBtn">Reload</button>
    <span class="muted" id="meta"></span>
  </div>
  <div id="tableWrap"></div>
<script>
let state = { rows: [], columns: [], sortBy: 'updated_at', desc: true, filterText: '' };

function cmpValue(a, b) {
  const da = Date.parse(a || '');
  const db = Date.parse(b || '');
  if (!Number.isNaN(da) && !Number.isNaN(db)) return da - db;
  return String(a || '').localeCompare(String(b || ''), undefined, { sensitivity: 'base' });
}

function applyView() {
  const filter = state.filterText.trim().toLowerCase();
  let rows = [...state.rows];
  if (filter) {
    rows = rows.filter(r => state.columns.some(c => String(r[c] || '').toLowerCase().includes(filter)));
  }
  rows.sort((a, b) => {
    const d = cmpValue(a[state.sortBy], b[state.sortBy]);
    return state.desc ? -d : d;
  });
  renderTable(rows);
  document.getElementById('meta').textContent = `${rows.length} / ${state.rows.length} rows`;
}

function renderTable(rows) {
  const wrap = document.getElementById('tableWrap');
  wrap.replaceChildren();
  if (!rows.length) {
    const empty = document.createElement('p');
    empty.textContent = '(empty)';
    wrap.appendChild(empty);
    return;
  }

  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const headRow = document.createElement('tr');
  const headerCols = [...state.columns, '_actions'];

  headerCols.forEach((c) => {
    const th = document.createElement('th');
    if (c === '_actions') {
      th.textContent = 'actions';
    } else {
      const btn = document.createElement('button');
      btn.setAttribute('data-sort', c);
      btn.textContent = `${c}${state.sortBy === c ? (state.desc ? ' ▼' : ' ▲') : ''}`;
      th.appendChild(btn);
    }
    headRow.appendChild(th);
  });

  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement('tbody');
  rows.forEach((r) => {
    const tr = document.createElement('tr');
    state.columns.forEach((c) => {
      const td = document.createElement('td');
      td.textContent = String(r[c] || '');
      tr.appendChild(td);
    });

    const actionsTd = document.createElement('td');
    const actionsDiv = document.createElement('div');
    actionsDiv.className = 'actions';
    ['start', 'done'].forEach((action) => {
      const btn = document.createElement('button');
      btn.setAttribute('data-job', String(r.job_id || ''));
      btn.setAttribute('data-action', action);
      btn.textContent = action;
      actionsDiv.appendChild(btn);
    });
    actionsTd.appendChild(actionsDiv);
    tr.appendChild(actionsTd);
    tbody.appendChild(tr);
  });

  table.appendChild(tbody);
  wrap.appendChild(table);

  wrap.querySelectorAll('button[data-sort]').forEach(btn => {
    btn.addEventListener('click', () => {
      const field = btn.getAttribute('data-sort');
      if (state.sortBy === field) state.desc = !state.desc;
      else { state.sortBy = field; state.desc = true; }
      applyView();
    });
  });

  wrap.querySelectorAll('button[data-action]').forEach(btn => {
    btn.addEventListener('click', async () => {
      const jobId = btn.getAttribute('data-job');
      const action = btn.getAttribute('data-action');
      btn.disabled = true;
      try {
        const resp = await fetch(`/api/${action}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ job_id: jobId }) });
        if (!resp.ok) throw new Error(await resp.text());
        await loadData();
      } catch (err) {
        alert(String(err));
      } finally {
        btn.disabled = false;
      }
    });
  });
}

async function loadData() {
  const resp = await fetch('/api/rows');
  const data = await resp.json();
  state.rows = data.rows || [];
  state.columns = data.columns || [];
  const sel = document.getElementById('sortField');
  const old = state.sortBy;
  sel.replaceChildren();
  state.columns.forEach((c) => {
    const opt = document.createElement('option');
    opt.value = c;
    opt.textContent = c;
    sel.appendChild(opt);
  });
  state.sortBy = state.columns.includes(old) ? old : (state.columns.includes('updated_at') ? 'updated_at' : state.columns[0]);
  sel.value = state.sortBy;
  applyView();
}

document.getElementById('filterText').addEventListener('input', (e) => { state.filterText = e.target.value || ''; applyView(); });
document.getElementById('sortField').addEventListener('change', (e) => { state.sortBy = e.target.value; applyView(); });
document.getElementById('ascBtn').addEventListener('click', () => { state.desc = false; applyView(); });
document.getElementById('descBtn').addEventListener('click', () => { state.desc = true; applyView(); });
document.getElementById('reloadBtn').addEventListener('click', loadData);
loadData();
</script>
</body>
</html>'''


def run_local_view(db_path: str, host: str = '127.0.0.1', port: int = 8765, open_browser: bool = True) -> None:
    class ViewHandler(BaseHTTPRequestHandler):
        def _write_json(self, code: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
            self.send_response(code)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict[str, Any]:
            length = int(self.headers.get('Content-Length', '0') or '0')
            raw = self.rfile.read(length) if length > 0 else b'{}'
            try:
                return json.loads(raw.decode('utf-8') or '{}')
            except json.JSONDecodeError:
                return {}

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path in ('/', '/index.html'):
                body = _view_html().encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path == '/api/rows':
                self._write_json(200, _view_payload(db_path))
                return
            self._write_json(404, {'error': 'not found'})

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            payload = self._read_json()
            job_id = str(payload.get('job_id', '')).strip()
            if not job_id:
                self._write_json(400, {'error': 'missing job_id'})
                return
            try:
                if path == '/api/start':
                    mark_start(job_id=job_id, db_path=db_path)
                    self._write_json(200, {'ok': True, 'job_id': job_id, 'status': 'start'})
                    return
                if path == '/api/done':
                    mark_done(job_id=job_id, db_path=db_path)
                    self._write_json(200, {'ok': True, 'job_id': job_id, 'status': 'done'})
                    return
            except Exception as exc:
                self._write_json(400, {'error': str(exc)})
                return
            self._write_json(404, {'error': 'not found'})

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer((host, port), ViewHandler)
    local_url = f'http://{host}:{port}/'
    print(f'Local view running at {local_url}')
    print('Press Ctrl+C to stop.')
    if open_browser:
        threading.Thread(target=lambda: webbrowser.open(local_url), daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def sync_push(db_path: str, remote: str) -> dict[str, Any]:
    remote_db = os.path.expanduser(remote)
    with tempfile.NamedTemporaryFile(prefix='mini_sim_db_push_', suffix='.json', delete=False) as tmp:
        artifact_path = tmp.name
    try:
        sync_export(db_path, artifact_path, include_all=True, mark_synced=False)
        out = sync_import(remote_db, artifact_path)
        out['remote'] = remote_db
        out['direction'] = 'push'
        return out
    finally:
        Path(artifact_path).unlink(missing_ok=True)


def sync_pull(db_path: str, remote: str) -> dict[str, Any]:
    remote_db = os.path.expanduser(remote)
    with tempfile.NamedTemporaryFile(prefix='mini_sim_db_pull_', suffix='.json', delete=False) as tmp:
        artifact_path = tmp.name
    try:
        sync_export(remote_db, artifact_path, include_all=True, mark_synced=False)
        out = sync_import(db_path, artifact_path)
        out['remote'] = remote_db
        out['direction'] = 'pull'
        return out
    finally:
        Path(artifact_path).unlink(missing_ok=True)


def _format_table(rows: list[dict[str, str]]) -> str:
    cols = ['case', 'status', 'job_id', 'bin', 'inp', 'updated_at', 'run_host', 'note']
    widths = {c: len(c) for c in cols}
    for row in rows:
        for c in cols:
            widths[c] = min(64, max(widths[c], len(str(row.get(c, '')))))
    header = ' | '.join(c.ljust(widths[c]) for c in cols)
    line = '-+-'.join('-' * widths[c] for c in cols)
    body = [' | '.join(str(row.get(c, '')).ljust(widths[c])[:widths[c]] for c in cols) for row in rows]
    return '\n'.join([header, line, *body])


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Mini simulation SQLite DB CLI',
        epilog=(
            'Examples:\n'
            '  ./sim_db init\n'
            '  ./sim_db add --case case_001 --inp case_001.inp --bin solver --status start\n'
            '  ./sim_db add --case case_001 --inp variant.inp --bin solver --status restart\n'
            '  ./sim_db done --job-id <job_id>\n'
            '  ./sim_db list --table\n'
            '  ./sim_db push /path/to/remote.sqlite3\n'
            '  ./sim_db pull /path/to/remote.sqlite3\n'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest='command', required=True)
    p_init = sub.add_parser('init', help='Initialize DB file')
    p_init.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to DB (CSV path auto-maps to SQLite)')

    p_add = sub.add_parser(
        'add',
        help='Add simulation item',
        description='Create a new simulation record. Multiple rows may share the same case. job_id is derived from case/work_dir/inp/input_files.',
        epilog=(
            'Examples:\n'
            '  ./sim_db add --case case_001 --inp case_001.inp --bin solver --status start\n'
            '  ./sim_db add --case case_001 --inp variant.inp --bin solver --status restart\n'
            '  ./sim_db add --case case_003 --inp a.inp --bin solver --status start --work-dir /tmp/case_003 --note "baseline run"\n'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p_add.add_argument('--case', required=True, help='Case label. Multiple rows may share the same case name.')
    p_add.add_argument('--inp', default=None, help='Primary input file. Convenience shortcut for the main input file.')
    p_add.add_argument('--input-file', action='append', default=[], help='Input file path. Repeat to store multiple inputs.')
    p_add.add_argument('--bin', dest='bin_name', required=True, help='Executable or solver binary name.')
    p_add.add_argument('--work-dir', '--wd', dest='work_dir', default=None, help='Working directory for this job. Defaults to current directory.')
    p_add.add_argument('--extra-param', action='append', default=[], help='Extra runtime parameter in key=value form. Repeatable.')
    p_add.add_argument('--extra-params', default=None, help='Raw extra runtime parameters string, for example JSON.')
    p_add.add_argument('--status', required=True, help='Initial status. Allowed: start, restart, done.')
    p_add.add_argument('--note', default='', help='Optional short note for the job.')
    p_add.add_argument('--notes', dest='note', help='Deprecated alias of --note kept for compatibility.')
    p_add.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to DB (CSV path auto-maps to SQLite)')

    p_done = sub.add_parser(
        'done',
        help='Mark a job as done',
        description='Switch a job status to done. Prefer --job-id. If a case label matches multiple rows, you must use --job-id.',
        epilog=(
            'Examples:\n'
            '  ./sim_db done --job-id 0123abcd4567ef89\n'
            '  ./sim_db done --case case_001\n'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    done_target = p_done.add_mutually_exclusive_group(required=True)
    done_target.add_argument('--case', help='Case label. Works only when it resolves to exactly one row.')
    done_target.add_argument('--job-id', dest='job_id', help='Stable job identifier. Preferred for unambiguous state changes.')
    p_done.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to DB (CSV path auto-maps to SQLite)')

    p_list = sub.add_parser('list', help='List simulation items')
    p_list.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to DB (CSV path auto-maps to SQLite)')
    p_list.add_argument('--status', default=None, help='Filter by status.')
    p_list.add_argument('--run-host', default=None, help='Filter by run_host.')
    p_list.add_argument('--sort-by', default='updated_at', help='Sort key. Default: updated_at.')
    p_list.add_argument('--asc', action='store_true', help='Sort ascending instead of descending.')
    p_list.add_argument('--limit', type=int, default=None, help='Maximum number of rows to show.')
    p_list.add_argument('--table', action='store_true', help='Show compact table view (easy inspection)')

    p_view = sub.add_parser('view', help='Open a local web UI for browsing/filtering/sorting rows and quick status actions')
    p_view.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to DB (CSV path auto-maps to SQLite)')
    p_view.add_argument('--host', default='127.0.0.1', help='Bind host for local web UI (default: 127.0.0.1)')
    p_view.add_argument('--port', type=int, default=8765, help='Bind port for local web UI (default: 8765)')
    p_view.add_argument('--no-open', action='store_true', help='Do not auto-open a browser tab')

    p_find = sub.add_parser(
        'find',
        help='Search simulation items',
        description='Case-insensitive search across rows. Bare text is treated like *text* automatically.',
        epilog=(
            'Examples:\n'
            '  ./sim_db find --text wing\n'
            '  ./sim_db find --case wing --work-dir project_a\n'
            '  ./sim_db find --input-file mesh --note baseline\n'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p_find.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to DB (CSV path auto-maps to SQLite)')
    p_find.add_argument('--text', default=None, help='General text search across case/work_dir/inp/input_files/note/bin. Case-insensitive.')
    p_find.add_argument('--case', dest='find_case', default=None, help='Case filter. Bare text behaves like *text*.')
    p_find.add_argument('--work-dir', dest='find_work_dir', default=None, help='Work dir filter. Bare text behaves like *text*.')
    p_find.add_argument('--inp', dest='find_inp', default=None, help='Primary inp filter. Bare text behaves like *text*.')
    p_find.add_argument('--input-file', dest='find_input_file', default=None, help='Input-files filter. Bare text behaves like *text*.')
    p_find.add_argument('--note', dest='find_note', default=None, help='Note filter. Bare text behaves like *text*.')
    p_find.add_argument('--bin', dest='find_bin', default=None, help='Binary filter. Bare text behaves like *text*.')
    p_find.add_argument('--status', dest='find_status', default=None, help='Status filter. Bare text behaves like *text*.')
    p_find.add_argument('--run-host', dest='find_run_host', default=None, help='Run host filter. Bare text behaves like *text*.')
    p_find.add_argument('--limit', type=int, default=None, help='Maximum number of rows to show.')
    p_find.add_argument('--table', action='store_true', help='Show compact table view.')

    p_import = sub.add_parser('import-csv', help='Import/merge rows from a legacy CSV file into SQLite DB')
    p_import.add_argument('--csv', required=True, help='Path to legacy CSV file')
    p_import.add_argument('--db', default=DEFAULT_DB_PATH, help='Target DB path (CSV path auto-maps to SQLite)')

    p_sync_status = sub.add_parser('sync-status', help='Show local sync status and pending records')
    p_sync_status.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to DB (CSV path auto-maps to SQLite)')
    p_sync_status.add_argument('--table', action='store_true', help='Show pending rows in compact table view')

    p_sync_export = sub.add_parser('sync-export', help='Export pending updates into a JSON sync artifact')
    p_sync_export.add_argument('--out', required=True, help='Output JSON file path')
    p_sync_export.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to DB (CSV path auto-maps to SQLite)')
    p_sync_export.add_argument('--all', action='store_true', help='Export all rows, not only pending ones')

    p_sync_import = sub.add_parser('sync-import', help='Import updates from a JSON sync artifact')
    p_sync_import.add_argument('--in', dest='in_path', required=True, help='Input JSON file path')
    p_sync_import.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to DB (CSV path auto-maps to SQLite)')

    p_push = sub.add_parser('push', help='Push local updates to a remote DB path')
    p_push.add_argument('remote', help='Remote DB path (for example /shared/remote.sqlite3)')
    p_push.add_argument('--db', default=DEFAULT_DB_PATH, help='Local DB path (CSV path auto-maps to SQLite)')

    p_pull = sub.add_parser('pull', help='Pull updates from a remote DB path into local DB')
    p_pull.add_argument('remote', help='Remote DB path (for example /shared/remote.sqlite3)')
    p_pull.add_argument('--db', default=DEFAULT_DB_PATH, help='Local DB path (CSV path auto-maps to SQLite)')
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_cli()
    args = parser.parse_args(argv)
    try:
        if args.command == 'init':
            init_sim_db(args.db)
        elif args.command == 'add':
            add_sim_item(case=args.case, inp=args.inp, input_files=args.input_file, bin_name=args.bin_name, status=args.status, db_path=args.db, note=args.note, work_dir=args.work_dir, extra_params=_normalize_extra_params(args.extra_params, args.extra_param))
        elif args.command == 'done':
            _, rows = _read_sim_db(args.db)
            target_job_id = resolve_job_id(rows, case=args.case, job_id=args.job_id)
            mark_done(job_id=target_job_id, db_path=args.db)
        elif args.command == 'list':
            rows = list_view(db_path=args.db, status=args.status, run_host=args.run_host, sort_by=args.sort_by, desc=not args.asc, limit=args.limit)
            if not rows:
                print('(empty)')
            elif args.table:
                print(_format_table(rows))
            else:
                for row in rows:
                    case = row.get('case', '')
                    detail = {k: v for k, v in row.items() if k != 'case'}
                    print(f'{case}: {detail}')
        elif args.command == 'find':
            rows = find_items(
                db_path=args.db,
                text=args.text,
                case=args.find_case,
                work_dir=args.find_work_dir,
                inp=args.find_inp,
                input_file=args.find_input_file,
                note=args.find_note,
                bin_name=args.find_bin,
                status=args.find_status,
                run_host=args.find_run_host,
                limit=args.limit,
            )
            if not rows:
                print('(empty)')
            elif args.table:
                print(_format_table(rows))
            else:
                for row in rows:
                    case = row.get('case', '')
                    detail = {k: v for k, v in row.items() if k != 'case'}
                    print(f'{case}: {detail}')
        elif args.command == 'view':
            run_local_view(db_path=args.db, host=args.host, port=args.port, open_browser=not args.no_open)
        elif args.command == 'import-csv':
            import_csv(args.csv, args.db)
        elif args.command == 'sync-status':
            status = sync_status(args.db)
            print(f"total={status['total_cases']} pending={status['pending_cases']} synced={status['synced_cases']} last_export={status['last_exported_at'] or '-'} last_import={status['last_imported_at'] or '-'}")
            pending_rows = status['pending']
            if pending_rows:
                if args.table:
                    print(_format_table(pending_rows))
                else:
                    for row in pending_rows:
                        case = row.get('case', '')
                        detail = {k: v for k, v in row.items() if k != 'case'}
                        print(f"{case}: {detail}")
            else:
                print('(no pending rows)')
        elif args.command == 'sync-export':
            out = sync_export(args.db, args.out, include_all=args.all)
            print(f"Exported {out['exported']} rows to {out['path']} at {out['exported_at']}")
        elif args.command == 'sync-import':
            out = sync_import(args.db, args.in_path)
            print(f"Imported {out['created']} new, {out['updated']} updated, {out['skipped']} unchanged from {out['imported_file']}")
            if out['conflicts']:
                print('Conflicts:')
                for conflict in out['conflicts']:
                    print(json.dumps(conflict, ensure_ascii=False, sort_keys=True))
        elif args.command == 'push':
            out = sync_push(args.db, args.remote)
            print(f"Pushed to {out['remote']}: {out['created']} created, {out['updated']} updated, {out['skipped']} unchanged")
            if out['conflicts']:
                print('Conflicts:')
                for conflict in out['conflicts']:
                    print(json.dumps(conflict, ensure_ascii=False, sort_keys=True))
        elif args.command == 'pull':
            out = sync_pull(args.db, args.remote)
            print(f"Pulled from {out['remote']}: {out['created']} created, {out['updated']} updated, {out['skipped']} unchanged")
            if out['conflicts']:
                print('Conflicts:')
                for conflict in out['conflicts']:
                    print(json.dumps(conflict, ensure_ascii=False, sort_keys=True))
        else:
            parser.print_help()
            return 1
    except (FileNotFoundError, ValueError, sqlite3.Error) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
