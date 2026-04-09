"""
simple database for simulations and more (CSV-backed CRUD)

author: hyharry@github
license: MIT License
version: 1.1
"""

__doc__ = 'simple database for simulations and more (CSV-backed CRUD)'

import argparse
import csv
import os
import re
import sys
from datetime import datetime
from typing import Any, Mapping

ALLOWED_STATUS = {'start', 'restart', 'done'}
DEFAULT_DB_PATH = os.path.expanduser('~/sim_db.csv')
CLI_FIELDS = ['inp', 'bin', 'status', 'notes', 'created_at', 'updated_at']


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _read_rows(fn_csv: str) -> tuple[list[str], list[dict[str, str]]]:
    if not os.path.exists(fn_csv):
        raise FileNotFoundError(f'Database not found: {fn_csv}')

    with open(fn_csv, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = [dict(row) for row in reader]
    return fieldnames, rows


def _write_rows(fn_csv: str, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with open(fn_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            clean = {k: row.get(k, '') for k in fieldnames}
            writer.writerow(clean)


def _ensure_case_field(fieldnames: list[str]) -> list[str]:
    if 'case' in fieldnames:
        return fieldnames
    return ['case', *fieldnames]


def _dict_table(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        case = row.get('case', '')
        if case:
            out[case] = {k: v for k, v in row.items() if k != 'case'}
    return out


def create_csv_db(fn_csv: str, dic: Mapping[str, Mapping[str, Any]]) -> None:
    """Create a new CSV-backed simulation database from a mapping of case records."""
    if os.path.exists(fn_csv):
        raise Exception(f'{fn_csv} already created, you can add items!')

    fields = {'case'}
    for detail in dic.values():
        fields.update(detail.keys())
    fieldnames = ['case', *sorted(f for f in fields if f != 'case')]

    rows: list[dict[str, str]] = []
    for case, detail in dic.items():
        row = {'case': case}
        for k, v in detail.items():
            row[k] = str(v)
        rows.append(row)

    _write_rows(fn_csv, fieldnames, rows)
    print(f'mini sim database: {fn_csv}, created! CREATE table')


def add_cases(fn_csv: str, sim_cases: Mapping[str, Mapping[str, Any]]) -> None:
    """Insert new simulation cases into an existing CSV database."""
    fieldnames, rows = _read_rows(fn_csv)
    fieldnames = _ensure_case_field(fieldnames)

    existing = {row.get('case', '') for row in rows}
    for detail in sim_cases.values():
        for key in detail.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    for cas, detail in sim_cases.items():
        if cas in existing:
            print(f'{cas} already in db (key), skip')
            continue
        row = {'case': cas}
        for k, v in detail.items():
            row[k] = str(v)
        rows.append(row)

    _write_rows(fn_csv, fieldnames, rows)
    print(f'mini sim database: {fn_csv}, updated! INSERT {len(sim_cases)} items, now total {len(rows)} items')


def add_case_info(fn_csv: str, new_info: str, case_val_d: Mapping[str, Any]) -> None:
    """Add a new column with per-case values for known case IDs."""
    fieldnames, rows = _read_rows(fn_csv)
    fieldnames = _ensure_case_field(fieldnames)
    if new_info not in fieldnames:
        fieldnames.append(new_info)

    for row in rows:
        case = row.get('case', '')
        if case in case_val_d:
            row[new_info] = str(case_val_d[case])

    _write_rows(fn_csv, fieldnames, rows)
    print(f"new info '{new_info}' added!")


def upd_cases(fn_csv: str, sim_cases_new_info: Mapping[str, Mapping[str, Any]]) -> None:
    """Update existing simulation cases with partial column/value mappings."""
    fieldnames, rows = _read_rows(fn_csv)
    fieldnames = _ensure_case_field(fieldnames)

    for detail in sim_cases_new_info.values():
        for col in detail.keys():
            if col not in fieldnames:
                fieldnames.append(col)

    by_case = {row.get('case', ''): row for row in rows}
    for cas, detail in sim_cases_new_info.items():
        if cas not in by_case:
            print(f'{cas} not present in db (key), skip')
            continue
        for col, val in detail.items():
            by_case[cas][col] = str(val)

    _write_rows(fn_csv, fieldnames, rows)
    print(f'mini sim database: {fn_csv}, updated! UPDATE {len(sim_cases_new_info)} sim cases')


def del_cases(fn_csv: str, sim_case_list: list[str]) -> None:
    """Delete simulation cases by case IDs from the CSV database."""
    fieldnames, rows = _read_rows(fn_csv)
    fieldnames = _ensure_case_field(fieldnames)

    delete_set = set(sim_case_list)
    kept: list[dict[str, str]] = []
    existing = {row.get('case', '') for row in rows}

    for cas in sim_case_list:
        if cas in existing:
            print(f'{cas} delete in db')
        else:
            print(f'{cas} not present in db (key), skip')

    for row in rows:
        if row.get('case', '') not in delete_set:
            kept.append(row)

    _write_rows(fn_csv, fieldnames, kept)
    print(f'mini sim database: {fn_csv}, changed! DELETE {len(sim_case_list)} items, now total {len(kept)} items')


def list_case_info(fn_csv: str) -> list[str]:
    """Print and return available column names for the simulation database."""
    fieldnames, _ = _read_rows(fn_csv)
    cols = [f for f in fieldnames if f != 'case']
    print(cols)
    return cols


def list_sim_db(fn_csv: str) -> dict[str, dict[str, str]]:
    """Print and return the full simulation database table as dict keyed by case."""
    _, rows = _read_rows(fn_csv)
    table = _dict_table(rows)
    print(table)
    return table


def search_sim_db(fn_csv: str, col_condition: str) -> list[str]:
    """Return case IDs matching a very simple condition: <col> == '<value>'."""
    _, rows = _read_rows(fn_csv)
    m = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*==\s*'([^']*)'\s*$", col_condition)
    if not m:
        raise ValueError("Only simple conditions like status == 'DONE' are supported")

    col, wanted = m.groups()
    out = []
    for row in rows:
        if row.get(col, '') == wanted:
            case = row.get('case', '')
            if case:
                out.append(case)
    return out


def _validate_status(status: str) -> None:
    if status not in ALLOWED_STATUS:
        allowed = ', '.join(sorted(ALLOWED_STATUS))
        raise ValueError(f"Invalid status '{status}'. Allowed: {allowed}")


def init_sim_db(db_path: str = DEFAULT_DB_PATH) -> None:
    """Create an empty simulation DB at db_path if it does not exist."""
    db_path = os.path.expanduser(db_path)
    if os.path.exists(db_path):
        print(f'Database already exists: {db_path}')
        return

    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    _write_rows(db_path, ['case', *CLI_FIELDS], [])
    print(f'Initialized database: {db_path}')


def _read_sim_db(db_path: str) -> tuple[list[str], list[dict[str, str]]]:
    db_path = os.path.expanduser(db_path)
    if not os.path.exists(db_path):
        raise FileNotFoundError(f'Database not found: {db_path}. Run init first.')
    return _read_rows(db_path)


def add_sim_item(
    case: str,
    inp: str,
    bin_name: str,
    status: str,
    db_path: str = DEFAULT_DB_PATH,
    notes: str = '',
) -> None:
    """Add one simulation item to the DB."""
    _validate_status(status)
    fieldnames, rows = _read_sim_db(db_path)
    fieldnames = _ensure_case_field(fieldnames)

    for field in CLI_FIELDS:
        if field not in fieldnames:
            fieldnames.append(field)

    if any(row.get('case', '') == case for row in rows):
        raise ValueError(f"Case '{case}' already exists in {os.path.expanduser(db_path)}")

    now = _now_iso()
    rows.append(
        {
            'case': case,
            'inp': inp,
            'bin': bin_name,
            'status': status,
            'notes': notes,
            'created_at': now,
            'updated_at': now,
        }
    )
    _write_rows(os.path.expanduser(db_path), fieldnames, rows)
    print(f"Added case '{case}' with status '{status}'")


def mark_done(case: str, db_path: str = DEFAULT_DB_PATH) -> None:
    """Mark a simulation case as done."""
    fieldnames, rows = _read_sim_db(db_path)
    fieldnames = _ensure_case_field(fieldnames)
    if 'status' not in fieldnames:
        fieldnames.append('status')
    if 'updated_at' not in fieldnames:
        fieldnames.append('updated_at')

    found = False
    for row in rows:
        if row.get('case', '') == case:
            row['status'] = 'done'
            row['updated_at'] = _now_iso()
            found = True
            break

    if not found:
        raise ValueError(f"Case '{case}' not found in {os.path.expanduser(db_path)}")

    _write_rows(os.path.expanduser(db_path), fieldnames, rows)
    print(f"Case '{case}' marked as done")


def list_items(db_path: str = DEFAULT_DB_PATH) -> dict[str, dict[str, str]]:
    """Return all records in the simulation DB as dict keyed by case."""
    _, rows = _read_sim_db(db_path)
    return _dict_table(rows)


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Mini simulation CSV DB CLI')
    sub = parser.add_subparsers(dest='command', required=True)

    p_init = sub.add_parser('init', help='Initialize DB file')
    p_init.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to CSV database (default: ~/sim_db.csv)')

    p_add = sub.add_parser('add', help='Add simulation item')
    p_add.add_argument('--case', required=True, help='Case name / unique key')
    p_add.add_argument('--inp', required=True, help='Input file or identifier')
    p_add.add_argument('--bin', dest='bin_name', required=True, help='Executable / binary name')
    p_add.add_argument('--status', required=True, help='start|restart|done')
    p_add.add_argument('--notes', default='', help='Optional free text notes')
    p_add.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to CSV database (default: ~/sim_db.csv)')

    p_done = sub.add_parser('done', help='Mark case status as done')
    p_done.add_argument('--case', required=True, help='Case name / unique key')
    p_done.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to CSV database (default: ~/sim_db.csv)')

    p_list = sub.add_parser('list', help='List simulation items')
    p_list.add_argument('--db', default=DEFAULT_DB_PATH, help='Path to CSV database (default: ~/sim_db.csv)')

    return parser


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
                bin_name=args.bin_name,
                status=args.status,
                db_path=args.db,
                notes=args.notes,
            )
        elif args.command == 'done':
            mark_done(case=args.case, db_path=args.db)
        elif args.command == 'list':
            items = list_items(args.db)
            if not items:
                print('(empty)')
            else:
                for case, detail in sorted(items.items()):
                    print(f'{case}: {detail}')
        else:
            parser.print_help()
            return 1
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2

    return 0


def simple_usage() -> None:
    """Run a minimal end-to-end demonstration of the CRUD APIs."""
    sim_db = dict(
        sim_a={'date_create': 20240101, 'directory': 'd_sim_a', 'exec_bin': 'prog_a', 'input_files': ['f1', 'f2'], 'status': 'DONE'},
        sim_b={'date_create': 20240201, 'directory': 'd_sim_a/b', 'exec_bin': 'prog_b', 'input_files': ['f5'], 'status': 'RUNNING'},
        sim_c={'date_create': 20240321, 'directory': 'd_sim_a/c/d', 'exec_bin': 'prog_c', 'input_files': ['f3', 'f4']},
    )
    fn_csv = 'test.csv'
    create_csv_db(fn_csv, sim_db)
    add_cases(fn_csv, {'dd': {'date_create': 1234}})
    del_cases(fn_csv, ['sim_b'])
    upd_cases(fn_csv, {'sim_c': {'status': 'RUNNING'}})
    add_case_info(fn_csv, 'restart', {'sim_a': False})
    list_case_info(fn_csv)
    search_sim_db(fn_csv, "status == 'DONE'")
    list_sim_db(fn_csv)


if __name__ == '__main__':
    raise SystemExit(main())
