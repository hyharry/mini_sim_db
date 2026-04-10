import os
import socket
import tempfile
import threading
import unittest

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
            )
            self.assertTrue(created["ok"])
            self.assertTrue(created["remote_ok"])
            self.assertTrue(created["local_ok"])

            one = client.read(case="c1")
            self.assertEqual(one["item"]["status"], "start")
            self.assertEqual(one["item"]["run_host"], socket.gethostname())

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
        )
        self.assertTrue(out["ok"])
        self.assertFalse(out["remote_ok"])
        self.assertEqual(out["fallback"], "local-only")

        local_table = list_items(self.local_db)
        self.assertIn("offline-c1", local_table)
        self.assertEqual(local_table["offline-c1"]["run_host"], socket.gethostname())

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


if __name__ == "__main__":
    unittest.main()
