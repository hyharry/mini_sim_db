import os
import socket
import tempfile
import threading
import time
import unittest
from unittest import mock

import remote_api.server as sim_db_server
from sim_db import list_items
from remote_api.client import SimDbClient
from remote_api.server import SecurityPolicy, SimDbApiServer


class RestServerTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_default = os.path.join(self.tmp.name, 'default.sqlite3')
        self.db_other = os.path.join(self.tmp.name, 'other.sqlite3')
        self.local_db = os.path.join(self.tmp.name, 'local.sqlite3')
        self.token = 'test-token'

    def tearDown(self):
        self.tmp.cleanup()

    def _start_server(self, *, allowed_db_path=None, allowed_base_dir=None):
        policy = SecurityPolicy(token=self.token, default_db_path=self.db_default, allowed_db_path=allowed_db_path, allowed_base_dir=allowed_base_dir)
        server = SimDbApiServer(('127.0.0.1', 0), policy)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = f'http://127.0.0.1:{server.server_port}'
        return server, thread, url

    def test_client_roundtrip_and_job_id_updates(self):
        server, thread, url = self._start_server()
        try:
            client = SimDbClient(base_url=url, token=self.token, local_db_path=self.local_db)
            self.assertTrue(client.init()['ok'])
            out = client.create(case='c1', inp='job.inp', input_files=['job.inp', 'mesh.inp'], bin_name='solver', status='start', note='remote submit', work_dir='/work/c1', extra_params='{"threads": 8}')
            self.assertTrue(out['ok'])
            one = client.read(case='c1')
            self.assertEqual(one['item']['status'], 'start')
            self.assertEqual(one['item']['run_host'], socket.gethostname())
            job_id = one['item']['job_id']
            updated = client.update(job_id=job_id, fields={'note': 'patched', 'status': 'restart'})
            self.assertTrue(updated['ok'])
            one2 = client.read(job_id=job_id)
            self.assertEqual(one2['item']['note'], 'patched')
            self.assertEqual(one2['item']['status'], 'restart')
            done = client.done(job_id=job_id)
            self.assertTrue(done['ok'])
            one3 = client.read(job_id=job_id)
            self.assertEqual(one3['item']['status'], 'done')
            deleted = client.delete(job_id=job_id)
            self.assertTrue(deleted['ok'])
            all_items = client.list()
            self.assertEqual(all_items['cases'], {})
        finally:
            server.shutdown(); server.server_close(); thread.join(timeout=2)

    def test_multiple_rows_same_case_require_job_id(self):
        server, thread, url = self._start_server()
        try:
            client = SimDbClient(base_url=url, token=self.token, local_db_path=self.local_db)
            client.init()
            client.create(case='dup', inp='a.inp', bin_name='solver', status='start', work_dir='/tmp/a')
            client.create(case='dup', inp='b.inp', bin_name='solver', status='start', work_dir='/tmp/b')
            with self.assertRaises(RuntimeError):
                client.read(case='dup')
        finally:
            server.shutdown(); server.server_close(); thread.join(timeout=2)

    def test_fallback_when_remote_unavailable(self):
        client = SimDbClient(base_url='http://127.0.0.1:1', token=self.token, local_db_path=self.local_db)
        out = client.create(case='offline-c1', inp='job.inp', bin_name='solver', status='start')
        self.assertTrue(out['ok'])
        self.assertFalse(out['remote_ok'])
        local_table = list_items(self.local_db)
        self.assertEqual(len(local_table), 1)

    def test_summary_endpoint(self):
        server, thread, url = self._start_server()
        try:
            client = SimDbClient(base_url=url, token=self.token, local_db_path=self.local_db)
            client.init()
            client.create(case='c1', inp='a.inp', bin_name='solver', status='start')
            client.create(case='c2', inp='b.inp', bin_name='solver', status='done')
            out = client.summary(status='done', limit=5)
            self.assertEqual(out['count'], 1)
            self.assertEqual(out['items'][0]['case'], 'c2')
        finally:
            server.shutdown(); server.server_close(); thread.join(timeout=2)

    def test_mutations_are_serialized(self):
        server, thread, url = self._start_server()
        overlap_detected = threading.Event()
        gate = threading.Lock()
        original_add = sim_db_server.add_sim_item

        def guarded_add(*args, **kwargs):
            if not gate.acquire(blocking=False):
                overlap_detected.set()
            else:
                try:
                    time.sleep(0.1)
                    return original_add(*args, **kwargs)
                finally:
                    gate.release()
        try:
            with mock.patch('remote_api.server.add_sim_item', side_effect=guarded_add):
                client = SimDbClient(base_url=url, token=self.token, enable_local_write=False)
                client.init()
                errors = []
                def _create(case, inp):
                    try:
                        client.create(case=case, inp=inp, bin_name='solver', status='start')
                    except Exception as exc:
                        errors.append(exc)
                t1 = threading.Thread(target=_create, args=('c1', 'a.inp'))
                t2 = threading.Thread(target=_create, args=('c2', 'b.inp'))
                t1.start(); t2.start(); t1.join(timeout=2); t2.join(timeout=2)
                self.assertEqual(errors, [])
                self.assertFalse(overlap_detected.is_set())
        finally:
            server.shutdown(); server.server_close(); thread.join(timeout=2)


if __name__ == '__main__':
    unittest.main()
