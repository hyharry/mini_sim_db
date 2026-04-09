import os
import sys
import tempfile
import subprocess
import unittest

from sim_db import (
    add_cases,
    add_sim_item,
    create_csv_db,
    del_cases,
    init_sim_db,
    list_items,
    list_sim_db,
    mark_done,
    upd_cases,
)


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
                'status': 'NEW',
            },
            'sim_b': {
                'date_create': 20240201,
                'directory': 'd_b',
                'exec_bin': 'prog_b',
                'input_files': ['f2'],
                'status': 'NEW',
            },
        }
        create_csv_db(self.fn_csv, self.initial_data)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_crud_flow(self):
        # verify create
        table = list_sim_db(self.fn_csv)
        self.assertEqual(set(table.keys()), {'sim_a', 'sim_b'})

        # add (insert)
        add_cases(self.fn_csv, {'sim_c': {'date_create': 20240301}})
        table = list_sim_db(self.fn_csv)
        self.assertIn('sim_c', table)

        # update
        upd_cases(self.fn_csv, {'sim_a': {'status': 'DONE'}})
        table = list_sim_db(self.fn_csv)
        self.assertEqual(table['sim_a']['status'], 'DONE')

        # delete
        del_cases(self.fn_csv, ['sim_b'])
        table = list_sim_db(self.fn_csv)
        self.assertNotIn('sim_b', table)


class TestSimpleCliFunctions(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, 'sim_db.csv')

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_init_add_done_list(self):
        init_sim_db(self.db_path)
        add_sim_item(
            case='case001',
            inp='job.inp',
            bin_name='solver.bin',
            status='start',
            db_path=self.db_path,
            notes='from test',
        )

        table = list_items(self.db_path)
        self.assertEqual(table['case001']['status'], 'start')

        mark_done('case001', self.db_path)
        table = list_items(self.db_path)
        self.assertEqual(table['case001']['status'], 'done')

    def test_status_validation(self):
        init_sim_db(self.db_path)
        with self.assertRaises(ValueError):
            add_sim_item(
                case='case002',
                inp='job.inp',
                bin_name='solver.bin',
                status='running',
                db_path=self.db_path,
            )

    def test_done_missing_case(self):
        init_sim_db(self.db_path)
        with self.assertRaises(ValueError):
            mark_done('missing_case', self.db_path)


class TestCliSubprocess(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.home_dir = self.tmp_dir.name
        self.project_dir = os.path.dirname(os.path.abspath(__file__))
        self.script = os.path.join(self.project_dir, 'sim_db.py')

    def tearDown(self):
        self.tmp_dir.cleanup()

    def _run(self, *args):
        env = os.environ.copy()
        env['HOME'] = self.home_dir
        return subprocess.run(
            [sys.executable, self.script, *args],
            cwd=self.project_dir,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_cli_default_db_path_and_invalid_status(self):
        r_init = self._run('init')
        self.assertEqual(r_init.returncode, 0, msg=r_init.stderr)
        self.assertTrue(os.path.exists(os.path.join(self.home_dir, 'sim_db.csv')))

        r_bad = self._run(
            'add',
            '--case', 'c1',
            '--inp', 'a.inp',
            '--bin', 'solver',
            '--status', 'RUNNING',
        )
        self.assertNotEqual(r_bad.returncode, 0)
        self.assertIn('Invalid status', r_bad.stderr)


if __name__ == '__main__':
    unittest.main()
