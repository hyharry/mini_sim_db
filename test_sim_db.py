import json
import os
import sys
import tempfile
import subprocess
import time
import unittest

from sim_db import _view_payload, add_sim_item, derive_job_id, find_items, import_csv, init_sim_db, list_items, list_view, mark_done, mark_start, resolve_job_id, sync_export, sync_import, sync_status


class TestSimpleCliFunctions(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, 'sim_db.csv')

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_init_add_done_list(self):
        init_sim_db(self.db_path)
        add_sim_item(case='case001', inp='job.inp', input_files=['job.inp', 'mesh.inp'], bin_name='solver.bin', status='start', db_path=self.db_path, note='from test', work_dir='/tmp/case001', extra_params='{"alpha": 1}')
        table = list_items(self.db_path)
        self.assertEqual(len(table), 1)
        row = next(iter(table.values()))
        self.assertEqual(row['case'], 'case001')
        self.assertEqual(row['status'], 'start')
        self.assertEqual(row['input_files'], 'job.inp;mesh.inp')
        self.assertEqual(row['note'], 'from test')
        self.assertEqual(row['job_id'], derive_job_id(case='case001', work_dir='/tmp/case001', inp='job.inp', input_files=['job.inp', 'mesh.inp']))
        before = row['updated_at']
        time.sleep(1)
        mark_done(job_id=row['job_id'], db_path=self.db_path)
        after = next(iter(list_items(self.db_path).values()))
        self.assertEqual(after['status'], 'done')
        self.assertNotEqual(before, after['updated_at'])

    def test_multiple_rows_same_case_supported(self):
        init_sim_db(self.db_path)
        add_sim_item(case='dup', inp='a.inp', bin_name='solver', status='start', db_path=self.db_path, work_dir='/tmp/a')
        add_sim_item(case='dup', inp='b.inp', bin_name='solver', status='restart', db_path=self.db_path, work_dir='/tmp/b')
        rows = list_view(self.db_path, sort_by='inp', desc=False)
        self.assertEqual(len(rows), 2)
        self.assertEqual([r['case'] for r in rows], ['dup', 'dup'])
        with self.assertRaises(ValueError):
            resolve_job_id(rows, case='dup')


    def test_find_items_case_insensitive_and_wildcard(self):
        init_sim_db(self.db_path)
        add_sim_item(case='Wing_Load', inp='mesh_A.inp', bin_name='solver', status='start', db_path=self.db_path, work_dir='/tmp/Project_A/run01', note='Baseline')
        add_sim_item(case='Tail_Load', inp='mesh_B.inp', bin_name='solver', status='restart', db_path=self.db_path, work_dir='/tmp/Project_B/run02', note='Follow up')
        rows = find_items(self.db_path, text='wing')
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['case'], 'Wing_Load')
        rows2 = find_items(self.db_path, case='*load', work_dir='project_a')
        self.assertEqual(len(rows2), 1)
        self.assertEqual(rows2[0]['case'], 'Wing_Load')
        rows3 = find_items(self.db_path, input_file='mesh_a', note='base')
        self.assertEqual(len(rows3), 1)

    def test_status_validation(self):
        init_sim_db(self.db_path)
        with self.assertRaises(ValueError):
            add_sim_item(case='case002', inp='job.inp', bin_name='solver.bin', status='running', db_path=self.db_path)

    def test_mark_start_and_view_payload(self):
        init_sim_db(self.db_path)
        add_sim_item(case='c3', inp='a.inp', bin_name='solver', status='done', db_path=self.db_path, note='n1')
        row = list_view(self.db_path)[0]
        mark_start(job_id=row['job_id'], db_path=self.db_path)
        row_after = list_view(self.db_path)[0]
        self.assertEqual(row_after['status'], 'start')
        payload = _view_payload(self.db_path)
        self.assertIn('rows', payload)
        self.assertIn('columns', payload)
        self.assertIn('status', payload['columns'])
        self.assertEqual(payload['rows'][0]['job_id'], row['job_id'])

    def test_csv_import(self):
        csv_file = os.path.join(self.tmp_dir.name, 'legacy.csv')
        with open(csv_file, 'w', encoding='utf-8') as f:
            f.write('case,bin,inp,status\nlegacy1,solver,legacy.inp,start\n')
        init_sim_db(self.db_path)
        import_csv(csv_file, self.db_path)
        rows = list_view(self.db_path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['case'], 'legacy1')


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
        return subprocess.run([sys.executable, self.script, *args], cwd=self.project_dir, env=env, capture_output=True, text=True, check=False)

    def test_cli_help_has_examples(self):
        r = self._run('--help')
        self.assertEqual(r.returncode, 0)
        self.assertIn('Examples:', r.stdout)
        self.assertIn('./sim_db done --job-id', r.stdout)


    def test_cli_find(self):
        self.assertEqual(self._run('init').returncode, 0)
        self.assertEqual(self._run('add', '--case', 'Wing_Load', '--inp', 'mesh_A.inp', '--bin', 'solver', '--status', 'start', '--work-dir', '/tmp/Project_A', '--note', 'Baseline').returncode, 0)
        found = self._run('find', '--text', 'wing', '--work-dir', 'project_a', '--table')
        self.assertEqual(found.returncode, 0)
        self.assertIn('Wing_Load', found.stdout)

    def test_cli_view_help(self):
        r = self._run('view', '--help')
        self.assertEqual(r.returncode, 0)
        self.assertIn('--no-open', r.stdout)

    def test_cli_done_by_job_id(self):
        self.assertEqual(self._run('init').returncode, 0)
        self.assertEqual(self._run('add', '--case', 'c2', '--inp', 'a.inp', '--bin', 'solver', '--status', 'start').returncode, 0)
        listed = self._run('list')
        self.assertEqual(listed.returncode, 0)
        marker = "'job_id': '"
        job_id = listed.stdout.split(marker, 1)[1].split("'", 1)[0]
        done = self._run('done', '--job-id', job_id)
        self.assertEqual(done.returncode, 0)
        self.assertIn('marked as done', done.stdout)

    def test_cli_rejects_ambiguous_case_done(self):
        self.assertEqual(self._run('init').returncode, 0)
        self.assertEqual(self._run('add', '--case', 'dup', '--inp', 'a.inp', '--bin', 'solver', '--status', 'start').returncode, 0)
        self.assertEqual(self._run('add', '--case', 'dup', '--inp', 'b.inp', '--bin', 'solver', '--status', 'start').returncode, 0)
        done = self._run('done', '--case', 'dup')
        self.assertNotEqual(done.returncode, 0)
        self.assertIn('matches multiple rows', done.stderr)


class TestLocalSync(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, 'sync.sqlite3')
        self.sync_file = os.path.join(self.tmp_dir.name, 'sync.json')
        init_sim_db(self.db_path)

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_sync_export_and_import(self):
        add_sim_item(case='s1', inp='a.inp', bin_name='solver', status='start', db_path=self.db_path)
        status_before = sync_status(self.db_path)
        self.assertEqual(status_before['pending_cases'], 1)
        out = sync_export(self.db_path, self.sync_file)
        self.assertEqual(out['exported'], 1)
        with open(self.sync_file, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        self.assertEqual(payload['count'], 1)
        imported = sync_import(self.db_path, self.sync_file)
        self.assertEqual(imported['skipped'], 1)


if __name__ == '__main__':
    unittest.main()
