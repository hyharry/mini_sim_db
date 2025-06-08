"""
simple database for simulations and more (based on pandas and include CRUD)

author: hyharry@github
license: MIT License
version: 1.0
"""

__doc__ = 'simple database for simulations and more (based on pandas and include CRUD)'

import os
import pandas as pd

def create_csv_db(fn_csv, dic):
    if os.path.exists(fn_csv): raise Exception(f'{fn_csv} already created, you can add items!')
    df = pd.DataFrame.from_dict(dic, orient='index')
    df.to_csv(fn_csv)
    print(f'mini sim database: {fn_csv}, created! CREATE table')

def add_cases(fn_csv, sim_cases):
    df = pd.read_csv(fn_csv, index_col=0)
    for cas, detail in sim_cases.items():
        if cas in df.index: 
            print(f'{cas} already in db (key), skip')
        else:
            df.loc[cas] = detail
    df.to_csv(fn_csv)
    print(f'mini sim database: {fn_csv}, updated! INSERT {len(sim_cases)} items, now total {len(df)} items')

def add_case_info(fn_csv, new_info, case_val_d):
    df = pd.read_csv(fn_csv, index_col=0)
    df[new_info] = case_val_d
    df.to_csv(fn_csv)
    print(f"new info '{new_info}' added!")
    print(df[new_info])

def upd_cases(fn_csv, sim_cases_new_info):
    df = pd.read_csv(fn_csv, index_col=0)
    for cas, detail in sim_cases_new_info.items():
        if cas not in df.index:
            print(f'{cas} not present in db (key), skip')
            continue
        df.loc[cas, detail.keys()] = detail.values()
    df.to_csv(fn_csv)
    print(f'mini sim database: {fn_csv}, updated! UPDATE {len(sim_cases_new_info)} sim cases')

def del_cases(fn_csv, sim_case_list):
    df = pd.read_csv(fn_csv, index_col=0)
    for cas in sim_case_list:
        if cas in df.index: 
            df = df.drop(cas)
            print(f'{cas} delete in db')
        else:
            print(f'{cas} not present in db (key), skip')
    df.to_csv(fn_csv)
    print(f'mini sim database: {fn_csv}, changed! DELETE {len(sim_case_list)} items, now total {len(df)} items')

def list_case_info(fn_csv):
    df = pd.read_csv(fn_csv, index_col=0)
    print(df.columns)
    return df

def list_sim_db(fn_csv):
    df = pd.read_csv(fn_csv, index_col=0)
    print(df)
    return df

def search_sim_db(fn_csv, col_condition):
    """ trick for filter out not nan: col_condition='col_name == col_name' """
    df = pd.read_csv(fn_csv, index_col=0)
    df_sel = df.query(col_condition)
    return df_sel.index

def simple_usage():
    sim_db = dict(
        sim_a = {'date_create': 20240101, 'directory': 'd_sim_a', 'exec_bin': 'prog_a','input_files': ['f1', 'f2'], 'status':'DONE'},
        sim_b = {'date_create': 20240201, 'directory': 'd_sim_a/b', 'exec_bin': 'prog_b','input_files': ['f5'], 'status':'RUNNING'},
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
