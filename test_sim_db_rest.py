import os
import tempfile
import threading
import unittest

from sim_db_client import SimDbClient
from sim_db_server import SecurityPolicy, SimDbApiServer


class RestServerTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_default = os.path.join(self.tmp.name, "default.csv")
        self.db_other = os.path.join(self.tmp.name, "other.csv")
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

    def test_client_roundtrip_default_db(self):
        server, thread, url = self._start_server()
        try:
            client = SimDbClient(base_url=url, token=self.token)
            self.assertTrue(client.health()["ok"])

            out = client.init()
            self.assertTrue(out["ok"])
            self.assertTrue(os.path.exists(self.db_default))

            client.add(
                case="c1",
                inp="job.inp",
                input_files=["job.inp", "mesh.inp"],
                bin_name="solver",
                status="start",
                note="remote submit",
                work_dir="/work/c1",
            )
            table = client.list()
            self.assertIn("c1", table["cases"])
            self.assertEqual(table["cases"]["c1"]["status"], "start")

            client.done(case="c1")
            table2 = client.list()
            self.assertEqual(table2["cases"]["c1"]["status"], "done")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_unauthorized_rejected(self):
        server, thread, url = self._start_server()
        try:
            bad = SimDbClient(base_url=url, token="wrong")
            with self.assertRaises(RuntimeError):
                bad.init()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_custom_db_denied_by_default(self):
        server, thread, url = self._start_server()
        try:
            client = SimDbClient(base_url=url, token=self.token)
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
            client = SimDbClient(base_url=url, token=self.token)
            client.init(db_path=self.db_other)
            self.assertTrue(os.path.exists(self.db_other))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
