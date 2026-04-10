import os
import sqlite3
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
    import_csv,
    init_sim_db,
    list_items,
    list_sim_db,
    list_view,
    mark_done,
    search_sim_db,
    upd_cases,
)


class TestCRUDOperations(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, 'test.csv')  # compatibility path
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
        create_csv_db(self.db_path, self.initial_data)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_crud_flow(self):
        table = list_sim_db(self.db_path)
        self.assertEqual(set(table.keys()), {'sim_a', 'sim_b'})

        add_cases(self.db_path, {'sim_c': {'date_create': 20240301}})
        table = list_sim_db(self.db_path)
        self.assertIn('sim_c', table)

        upd_cases(self.db_path, {'sim_a': {'status': 'DONE'}})
        table = list_sim_db(self.db_path)
        self.assertEqual(table['sim_a']['status'], 'DONE')

        del_cases(self.db_path, ['sim_b'])
        table = list_sim_db(self.db_path)
        self.assertNotIn('sim_b', table)

    def test_delete_case_cascades_extra_rows_and_search(self):
        add_cases(self.db_path, {'sim_c': {'status': 'NEW', 'owner': 'alice'}})
        self.assertEqual(search_sim_db(self.db_path, "owner == 'alice'"), ['sim_c'])

        del_cases(self.db_path, ['sim_c'])

        sqlite_path = os.path.join(self.tmp_dir.name, 'test.sqlite3')
        conn = sqlite3.connect(sqlite_path)
        try:
            leftovers = conn.execute('SELECT COUNT(*) FROM sim_case_extra WHERE "case" = ?', ('sim_c',)).fetchone()[0]
        finally:
            conn.close()

        self.assertEqual(leftovers, 0)
        self.assertEqual(search_sim_db(self.db_path, "owner == 'alice'"), [])
        self.assertNotIn('sim_c', list_sim_db(self.db_path))


class TestSimpleCliFunctions(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, 'sim_db.csv')

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_init_add_done_list(self):
        init_sim_db(self.db_path)
        self.assertTrue(os.path.exists(os.path.join(self.tmp_dir.name, 'sim_db.sqlite3')))

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
        self.assertRegex(table['case001']['job_id'], r'^[0-9a-f]{16}$')

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

    def test_view_filter(self):
        init_sim_db(self.db_path)
        add_sim_item(case='a', inp='a.inp', bin_name='solver', status='start', db_path=self.db_path, work_dir='/tmp/a')
        add_sim_item(case='b', inp='b.inp', bin_name='solver', status='done', db_path=self.db_path, work_dir='/tmp/b')
        rows = list_view(self.db_path, status='done')
        self.assertEqual([r['case'] for r in rows], ['b'])

    def test_csv_import(self):
        csv_file = os.path.join(self.tmp_dir.name, 'legacy.csv')
        with open(csv_file, 'w', encoding='utf-8') as f:
            f.write('case,bin,inp,status\n')
            f.write('legacy1,solver,legacy.inp,start\n')
        init_sim_db(self.db_path)
        import_csv(csv_file, self.db_path)
        self.assertIn('legacy1', list_items(self.db_path))


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
        self.assertTrue(os.path.exists(os.path.join(self.home_dir, 'sim_db.sqlite3')))

        r_bad = self._run(
            'add',
            '--case', 'c1',
            '--inp', 'a.inp',
            '--bin', 'solver',
            '--status', 'RUNNING',
        )
        self.assertNotEqual(r_bad.returncode, 0)
        self.assertIn('Invalid status', r_bad.stderr)

    def test_cli_table_view(self):
        self.assertEqual(self._run('init').returncode, 0)
        self.assertEqual(
            self._run(
                'add', '--case', 'c2', '--inp', 'a.inp', '--bin', 'solver', '--status', 'start'
            ).returncode,
            0,
        )
        r_list = self._run('list', '--table')
        self.assertEqual(r_list.returncode, 0, msg=r_list.stderr)
        self.assertIn('case', r_list.stdout)
        self.assertIn('c2', r_list.stdout)


if __name__ == '__main__':
    unittest.main()
