import csv
import os
import sys
import tempfile
import subprocess
import time
import unittest

from sim_db import (
    add_cases,
    add_sim_item,
    create_csv_db,
    del_cases,
    derive_job_id,
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

    def _header(self):
        with open(self.db_path, 'r', newline='', encoding='utf-8') as f:
            return next(csv.reader(f))

    def test_init_add_done_list(self):
        init_sim_db(self.db_path)
        add_sim_item(
            case='case001',
            inp='job.inp',
            input_files=['job.inp', 'mesh.inp'],
            bin_name='solver.bin',
            status='start',
            db_path=self.db_path,
            note='from test',
            work_dir='/tmp/case001',
            extra_params='{"alpha": 1, "beta": "x"}',
        )

        table = list_items(self.db_path)
        self.assertEqual(table['case001']['status'], 'start')
        self.assertEqual(table['case001']['inp'], 'job.inp')
        self.assertEqual(table['case001']['input_files'], 'job.inp;mesh.inp')
        self.assertEqual(table['case001']['note'], 'from test')
        self.assertEqual(table['case001']['work_dir'], '/tmp/case001')
        self.assertEqual(table['case001']['extra_params'], '{"alpha": 1, "beta": "x"}')
        self.assertEqual(table['case001']['state_changed_at'], table['case001']['updated_at'])
        self.assertEqual(
            table['case001']['job_id'],
            derive_job_id(
                case='case001',
                work_dir='/tmp/case001',
                inp='job.inp',
                input_files=['job.inp', 'mesh.inp'],
            ),
        )

        before_change = table['case001']['state_changed_at']
        time.sleep(1)
        mark_done('case001', self.db_path)

        table = list_items(self.db_path)
        self.assertEqual(table['case001']['status'], 'done')
        self.assertNotEqual(before_change, table['case001']['state_changed_at'])
        self.assertEqual(table['case001']['state_changed_at'], table['case001']['updated_at'])

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

    def test_input_required(self):
        init_sim_db(self.db_path)
        with self.assertRaises(ValueError):
            add_sim_item(
                case='case003',
                inp=None,
                input_files=[],
                bin_name='solver.bin',
                status='start',
                db_path=self.db_path,
            )

    def test_done_missing_case(self):
        init_sim_db(self.db_path)
        with self.assertRaises(ValueError):
            mark_done('missing_case', self.db_path)

    def test_column_order_is_stable(self):
        init_sim_db(self.db_path)
        add_sim_item(
            case='case004',
            inp='x.inp',
            bin_name='solver.bin',
            status='start',
            db_path=self.db_path,
            work_dir='/tmp/work',
        )

        self.assertEqual(
            self._header(),
            [
                'case',
                'work_dir',
                'bin',
                'inp',
                'input_files',
                'job_id',
                'extra_params',
                'status',
                'note',
                'notes',
                'state_changed_at',
                'created_at',
                'updated_at',
            ],
        )


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

    def test_cli_input_files_note_work_dir(self):
        self.assertEqual(self._run('init').returncode, 0)

        r_add = self._run(
            'add',
            '--case', 'c2',
            '--inp', 'base.inp',
            '--input-file', 'extra1.inp',
            '--input-file', 'extra2.inp',
            '--bin', 'solver',
            '--work-dir', '/tmp/c2',
            '--extra-param', 'threads=8',
            '--extra-param', 'precision=double',
            '--status', 'start',
            '--note', 'short note',
        )
        self.assertEqual(r_add.returncode, 0, msg=r_add.stderr)

        r_list = self._run('list')
        self.assertEqual(r_list.returncode, 0, msg=r_list.stderr)
        self.assertIn("'inp': 'base.inp'", r_list.stdout)
        self.assertIn("'input_files': 'base.inp;extra1.inp;extra2.inp'", r_list.stdout)
        self.assertIn("'note': 'short note'", r_list.stdout)
        self.assertIn("'work_dir': '/tmp/c2'", r_list.stdout)
        self.assertIn("'extra_params': '{\"precision\": \"double\", \"threads\": \"8\"}'", r_list.stdout)
        self.assertIn("'state_changed_at': '", r_list.stdout)

    def test_cli_extra_params_conflict_rejected(self):
        self.assertEqual(self._run('init').returncode, 0)
        r_add = self._run(
            'add',
            '--case', 'c3',
            '--inp', 'base.inp',
            '--bin', 'solver',
            '--status', 'start',
            '--extra-param', 'threads=8',
            '--extra-params', '{"threads": 16}',
        )
        self.assertNotEqual(r_add.returncode, 0)
        self.assertIn('Use either --extra-params or --extra-param, not both', r_add.stderr)

    def test_cli_done_by_job_id(self):
        self.assertEqual(self._run('init').returncode, 0)
        self.assertEqual(
            self._run(
                'add',
                '--case', 'c4',
                '--inp', 'base.inp',
                '--bin', 'solver',
                '--status', 'start',
                '--work-dir', '/tmp/c4',
            ).returncode,
            0,
        )

        job_id = list_items(os.path.join(self.home_dir, 'sim_db.csv'))['c4']['job_id']
        r_done = self._run('done', '--job-id', job_id)
        self.assertEqual(r_done.returncode, 0, msg=r_done.stderr)
        self.assertEqual(list_items(os.path.join(self.home_dir, 'sim_db.csv'))['c4']['status'], 'done')


if __name__ == '__main__':
    unittest.main()
