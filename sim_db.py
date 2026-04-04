"""
simple database for simulations and more (based on pandas and include CRUD)

author: hyharry@github
license: MIT License
version: 1.0
"""

__doc__ = 'simple database for simulations and more (based on pandas and include CRUD)'

import os
from typing import Any, Mapping

import pandas as pd


def create_csv_db(fn_csv: str, dic: Mapping[str, Mapping[str, Any]]) -> None:
    """Create a new CSV-backed simulation database from a mapping of case records."""
    if os.path.exists(fn_csv):
        raise Exception(f'{fn_csv} already created, you can add items!')
    df = pd.DataFrame.from_dict(dic, orient='index')
    df.to_csv(fn_csv)
    print(f'mini sim database: {fn_csv}, created! CREATE table')


def add_cases(fn_csv: str, sim_cases: Mapping[str, Mapping[str, Any]]) -> None:
    """Insert new simulation cases into an existing CSV database."""
    df = pd.read_csv(fn_csv, index_col=0)
    for cas, detail in sim_cases.items():
        if cas in df.index:
            print(f'{cas} already in db (key), skip')
        else:
            df.loc[cas] = detail
    df.to_csv(fn_csv)
    print(f'mini sim database: {fn_csv}, updated! INSERT {len(sim_cases)} items, now total {len(df)} items')


def add_case_info(fn_csv: str, new_info: str, case_val_d: Mapping[str, Any]) -> None:
    """Add a new column with per-case values for known case IDs."""
    df = pd.read_csv(fn_csv, index_col=0)
    df[new_info] = case_val_d
    df.to_csv(fn_csv)
    print(f"new info '{new_info}' added!")
    print(df[new_info])


def upd_cases(fn_csv: str, sim_cases_new_info: Mapping[str, Mapping[str, Any]]) -> None:
    """Update existing simulation cases with partial column/value mappings."""
    df = pd.read_csv(fn_csv, index_col=0)
    for cas, detail in sim_cases_new_info.items():
        if cas not in df.index:
            print(f'{cas} not present in db (key), skip')
            continue
        # assign each updated value to the correct column
        for col, val in detail.items():
            df.loc[cas, col] = val
    df.to_csv(fn_csv)
    print(f'mini sim database: {fn_csv}, updated! UPDATE {len(sim_cases_new_info)} sim cases')


def del_cases(fn_csv: str, sim_case_list: list[str]) -> None:
    """Delete simulation cases by case IDs from the CSV database."""
    df = pd.read_csv(fn_csv, index_col=0)
    for cas in sim_case_list:
        if cas in df.index:
            df = df.drop(cas)
            print(f'{cas} delete in db')
        else:
            print(f'{cas} not present in db (key), skip')
    df.to_csv(fn_csv)
    print(f'mini sim database: {fn_csv}, changed! DELETE {len(sim_case_list)} items, now total {len(df)} items')


def list_case_info(fn_csv: str) -> pd.DataFrame:
    """Print and return available column names for the simulation database."""
    df = pd.read_csv(fn_csv, index_col=0)
    print(df.columns)
    return df


def list_sim_db(fn_csv: str) -> pd.DataFrame:
    """Print and return the full simulation database table."""
    df = pd.read_csv(fn_csv, index_col=0)
    print(df)
    return df


def search_sim_db(fn_csv: str, col_condition: str) -> pd.Index:
    """Return case IDs matching a pandas query expression.

    Use expressions such as ``status == 'DONE'`` or ``status == status``
    to filter out NaN rows.
    """
    df = pd.read_csv(fn_csv, index_col=0)
    df_sel = df.query(col_condition)
    return df_sel.index


def simple_usage() -> None:
    """Run a minimal end-to-end demonstration of the CRUD APIs."""
    sim_db = dict(
        sim_a = {'date_create': 20240101, 'directory': 'd_sim_a', 'exec_bin': 'prog_a', 'input_files': ['f1', 'f2'], 'status':'DONE'},
        sim_b = {'date_create': 20240201, 'directory': 'd_sim_a/b', 'exec_bin': 'prog_b', 'input_files': ['f5'], 'status':'RUNNING'},
        sim_c = {'date_create': 20240321, 'directory': 'd_sim_a/c/d', 'exec_bin': 'prog_c','input_files': ['f3', 'f4']},
    )
    fn_csv = 'test.csv'
    create_csv_db(fn_csv, sim_db)
    add_cases(fn_csv, {'dd':{'date_create': 1234}})
    del_cases(fn_csv, ['sim_b'])
    upd_cases(fn_csv, {'sim_c':{'status': 'RUNNING'}})
    add_case_info(fn_csv, 'restart', {'sim_a': False})
    list_case_info(fn_csv)
    search_sim_db(fn_csv, "status == 'DONE'")
    list_sim_db(fn_csv)

if __name__ == "__main__":
    simple_usage()
