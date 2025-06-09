import os
import unittest
import tempfile
import pandas as pd

from sim_db import create_csv_db, add_cases, upd_cases, del_cases, list_sim_db


class TestCRUDOperations(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.fn_csv = os.path.join(self.tmp_dir.name, 'test.csv')
        self.initial_data = {
            'sim_a': {
                'date_create': 20240101,
                'directory': 'd_a',
                'exec_bin': 'prog_a',
                'input_files': ['f1'],
                'status': 'NEW'
            },
            'sim_b': {
                'date_create': 20240201,
                'directory': 'd_b',
                'exec_bin': 'prog_b',
                'input_files': ['f2'],
                'status': 'NEW'
            }
        }
        create_csv_db(self.fn_csv, self.initial_data)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_crud_flow(self):
        # verify create
        df = list_sim_db(self.fn_csv)
        self.assertEqual(set(df.index), {'sim_a', 'sim_b'})

        # add (insert)
        add_cases(self.fn_csv, {'sim_c': {'date_create': 20240301}})
        df = list_sim_db(self.fn_csv)
        self.assertIn('sim_c', df.index)

        # update
        upd_cases(self.fn_csv, {'sim_a': {'status': 'DONE'}})
        df = list_sim_db(self.fn_csv)
        self.assertEqual(df.loc['sim_a', 'status'], 'DONE')

        # delete
        del_cases(self.fn_csv, ['sim_b'])
        df = list_sim_db(self.fn_csv)
        self.assertNotIn('sim_b', df.index)


if __name__ == '__main__':
    unittest.main()
