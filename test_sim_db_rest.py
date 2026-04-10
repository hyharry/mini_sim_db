import os
import socket
import tempfile
import threading
import time
import unittest
from unittest import mock

import sim_db_server
from sim_db import list_items
from sim_db_client import SimDbClient
from sim_db_server import SecurityPolicy, SimDbApiServer


class RestServerTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_default = os.path.join(self.tmp.name, "default.csv")
        self.db_other = os.path.join(self.tmp.name, "other.csv")
        self.local_db = os.path.join(self.tmp.name, "local.csv")
        self.token = "test-token"

    def tearDown(self):
        self.tmp.cleanup()

    def _start_server(self, *, allowed_db_path=None, allowed_base_dir=None):
        policy = SecurityPolicy(
            token=self.token,
            default_db_path=self.db_default,
            allowed_db_path=allowed_db_path,
            allowed_base_dir=allowed_base_dir,
        )
        server = SimDbApiServer(("127.0.0.1", 0), policy)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = f"http://127.0.0.1:{server.server_port}"
        return server, thread, url

    def test_client_full_crud_roundtrip_and_run_host(self):
        server, thread, url = self._start_server()
        try:
            client = SimDbClient(base_url=url, token=self.token, local_db_path=self.local_db)
            self.assertTrue(client.health()["ok"])

            out = client.init()
            self.assertTrue(out["ok"])
            self.assertTrue(os.path.exists(self.db_default))

            created = client.create(
                case="c1",
                inp="job.inp",
                input_files=["job.inp", "mesh.inp"],
                bin_name="solver",
                status="start",
                note="remote submit",
                work_dir="/work/c1",
                extra_params='{"threads": 8, "precision": "double"}',
            )
            self.assertTrue(created["ok"])
            self.assertTrue(created["remote_ok"])
            self.assertTrue(created["local_ok"])

            one = client.read(case="c1")
            self.assertEqual(one["item"]["status"], "start")
            self.assertEqual(one["item"]["run_host"], socket.gethostname())
            self.assertEqual(one["item"]["extra_params"], '{"threads": 8, "precision": "double"}')

            updated = client.update(case="c1", fields={"note": "patched", "status": "restart"})
            self.assertTrue(updated["ok"])
            one2 = client.read(case="c1")
            self.assertEqual(one2["item"]["note"], "patched")
            self.assertEqual(one2["item"]["status"], "restart")
            self.assertEqual(one2["item"]["run_host"], socket.gethostname())

            done = client.done(case="c1")
            self.assertTrue(done["ok"])
            one3 = client.read(case="c1")
            self.assertEqual(one3["item"]["status"], "done")

            deleted = client.delete(case="c1")
            self.assertTrue(deleted["ok"])
            all_items = client.list()
            self.assertNotIn("c1", all_items["cases"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_dual_write_fallback_when_remote_unavailable(self):
        client = SimDbClient(
            base_url="http://127.0.0.1:1",
            token=self.token,
            local_db_path=self.local_db,
        )

        out = client.create(
            case="offline-c1",
            inp="job.inp",
            bin_name="solver",
            status="start",
            extra_params='{"mode": "offline"}',
        )
        self.assertTrue(out["ok"])
        self.assertFalse(out["remote_ok"])
        self.assertEqual(out["fallback"], "local-only")

        local_table = list_items(self.local_db)
        self.assertIn("offline-c1", local_table)
        self.assertEqual(local_table["offline-c1"]["run_host"], socket.gethostname())
        self.assertEqual(local_table["offline-c1"]["extra_params"], '{"mode": "offline"}')

    def test_unauthorized_rejected(self):
        server, thread, url = self._start_server()
        try:
            bad = SimDbClient(base_url=url, token="wrong", local_db_path=self.local_db)
            with self.assertRaises(RuntimeError):
                bad.init()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_dual_write_does_not_fallback_on_http_error(self):
        server, thread, url = self._start_server()
        try:
            bad = SimDbClient(base_url=url, token="wrong", local_db_path=self.local_db)
            with self.assertRaisesRegex(RuntimeError, "HTTP 401"):
                bad.create(case="auth-c1", inp="job.inp", bin_name="solver", status="start")

            local_table = list_items(self.local_db)
            self.assertIn("auth-c1", local_table)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_custom_db_denied_by_default(self):
        server, thread, url = self._start_server()
        try:
            client = SimDbClient(base_url=url, token=self.token, local_db_path=self.local_db)
            client.init()
            with self.assertRaises(RuntimeError):
                client.init(db_path=self.db_other)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_custom_db_allowed_with_base_dir(self):
        server, thread, url = self._start_server(allowed_base_dir=self.tmp.name)
        try:
            client = SimDbClient(base_url=url, token=self.token, local_db_path=self.local_db)
            client.init(db_path=self.db_other)
            self.assertTrue(os.path.exists(self.db_other))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_case_path_roundtrip_with_url_escaped_case_id(self):
        server, thread, url = self._start_server()
        try:
            client = SimDbClient(base_url=url, token=self.token, local_db_path=self.local_db)
            client.init()
            case = "folder/name with space"
            client.create(case=case, inp="job.inp", bin_name="solver", status="start")

            one = client.read(case=case)
            self.assertEqual(one["case"], case)

            client.update(case=case, fields={"note": "updated"})
            one2 = client.read(case=case)
            self.assertEqual(one2["item"]["note"], "updated")

            client.delete(case=case)
            all_items = client.list()
            self.assertNotIn(case, all_items["cases"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_mutations_are_serialized_under_threaded_server(self):
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
            with mock.patch("sim_db_server.add_sim_item", side_effect=guarded_add):
                client = SimDbClient(base_url=url, token=self.token, enable_local_write=False)
                client.init()

                errors: list[Exception] = []

                def _create(case: str, inp: str) -> None:
                    try:
                        client.create(case=case, inp=inp, bin_name="solver", status="start")
                    except Exception as exc:  # pragma: no cover - defensive for thread join assertions
                        errors.append(exc)

                t1 = threading.Thread(target=_create, args=("c1", "a.inp"))
                t2 = threading.Thread(target=_create, args=("c2", "b.inp"))
                t1.start()
                t2.start()
                t1.join(timeout=2)
                t2.join(timeout=2)

                self.assertFalse(t1.is_alive())
                self.assertFalse(t2.is_alive())
                self.assertEqual(errors, [])
                self.assertFalse(overlap_detected.is_set(), "mutating requests overlapped unexpectedly")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_patch_rejects_case_mutation_in_fields(self):
        server, thread, url = self._start_server()
        try:
            client = SimDbClient(base_url=url, token=self.token, local_db_path=self.local_db)
            client.init()
            client.create(case="c1", inp="a.inp", bin_name="solver", status="start")
            with self.assertRaisesRegex(RuntimeError, "immutable"):
                client.update(case="c1", fields={"case": "c2"})
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_read_waits_for_mutation_lock(self):
        server, thread, url = self._start_server()
        gate = threading.Event()
        release = threading.Event()

        original_list = sim_db_server.list_items

        def guarded_list(db_path):
            if not gate.is_set():
                gate.set()
                release.wait(timeout=2)
            return original_list(db_path)

        try:
            client = SimDbClient(base_url=url, token=self.token, enable_local_write=False)
            client.init()
            client.create(case="c1", inp="a.inp", bin_name="solver", status="start")

            with mock.patch("sim_db_server.list_items", side_effect=guarded_list):
                writer_done = threading.Event()

                def _update():
                    client.update(case="c1", fields={"note": "x"})
                    writer_done.set()

                t = threading.Thread(target=_update)
                t.start()
                gate.wait(timeout=2)

                read_done = threading.Event()

                def _read():
                    client.list()
                    read_done.set()

                t_read = threading.Thread(target=_read)
                t_read.start()
                time.sleep(0.1)
                self.assertFalse(read_done.is_set())

                release.set()
                t.join(timeout=2)
                t_read.join(timeout=2)
                self.assertTrue(writer_done.is_set())
                self.assertTrue(read_done.is_set())
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_create_rejects_user_supplied_job_id(self):
        server, thread, url = self._start_server()
        try:
            client = SimDbClient(base_url=url, token=self.token, local_db_path=self.local_db)
            client.init()
            with self.assertRaisesRegex(RuntimeError, "auto-generated"):
                client._request(
                    "POST",
                    "/cases",
                    {
                        "case": "c-bad",
                        "inp": "a.inp",
                        "bin_name": "solver",
                        "status": "start",
                        "job_id": "manual-id",
                    },
                )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_job_id_can_be_used_for_follow_up_updates(self):
        server, thread, url = self._start_server()
        try:
            client = SimDbClient(base_url=url, token=self.token, local_db_path=self.local_db)
            client.init()
            client.create(case="c1", inp="a.inp", input_files=["a.inp", "b.inp"], bin_name="solver", status="start", work_dir="/tmp/w")
            first = client.read(case="c1")
            job_id = first["item"]["job_id"]

            client.update(job_id=job_id, fields={"note": "by job id", "status": "restart"})
            second = client.read(job_id=job_id)
            self.assertEqual(second["case"], "c1")
            self.assertEqual(second["item"]["note"], "by job id")
            self.assertEqual(second["item"]["status"], "restart")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
