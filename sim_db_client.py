"""Tiny REST client for mini_sim_db server."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any
from urllib import error, parse, request


class SimDbClient:
    def __init__(self, base_url: str, token: str, timeout: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/health")

    def init(self, db_path: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if db_path:
            payload["db_path"] = db_path
        return self._request("POST", "/init", payload)

    def add(
        self,
        *,
        case: str,
        bin_name: str,
        status: str,
        inp: str | None = None,
        input_files: list[str] | None = None,
        note: str | None = None,
        work_dir: str | None = None,
        db_path: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "case": case,
            "bin_name": bin_name,
            "status": status,
        }
        if inp is not None:
            payload["inp"] = inp
        if input_files is not None:
            payload["input_files"] = input_files
        if note is not None:
            payload["note"] = note
        if work_dir is not None:
            payload["work_dir"] = work_dir
        if db_path is not None:
            payload["db_path"] = db_path
        return self._request("POST", "/add", payload)

    def done(self, *, case: str, db_path: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"case": case}
        if db_path is not None:
            payload["db_path"] = db_path
        return self._request("POST", "/done", payload)

    def list(self, db_path: str | None = None) -> dict[str, Any]:
        path = "/cases"
        if db_path:
            path += "?" + parse.urlencode({"db_path": db_path})
        return self._request("GET", path)

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None
        headers = {"Authorization": f"Bearer {self.token}"}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(self.base_url + path, method=method, headers=headers, data=data)
        try:
            with request.urlopen(req, timeout=self.timeout) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body) if body else {}
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8")
            msg = body or str(exc)
            raise RuntimeError(f"HTTP {exc.code}: {msg}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"request failed: {exc}") from exc


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="mini_sim_db REST client")
    p.add_argument("--url", default="http://127.0.0.1:8765", help="Server base URL")
    p.add_argument("--token", default=None, help="Bearer token (or SIM_DB_API_TOKEN)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("health")

    p_init = sub.add_parser("init")
    p_init.add_argument("--db", default=None, help="Optional db_path override")

    p_add = sub.add_parser("add")
    p_add.add_argument("--case", required=True)
    p_add.add_argument("--bin", dest="bin_name", required=True)
    p_add.add_argument("--status", required=True)
    p_add.add_argument("--inp", default=None)
    p_add.add_argument("--input-file", action="append", default=None)
    p_add.add_argument("--note", default=None)
    p_add.add_argument("--work-dir", default=None)
    p_add.add_argument("--db", default=None)

    p_done = sub.add_parser("done")
    p_done.add_argument("--case", required=True)
    p_done.add_argument("--db", default=None)

    p_list = sub.add_parser("list")
    p_list.add_argument("--db", default=None)

    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    token = args.token or os.getenv("SIM_DB_API_TOKEN")
    if not token:
        print("Missing token. Set --token or SIM_DB_API_TOKEN", file=sys.stderr)
        return 2

    client = SimDbClient(base_url=args.url, token=token)
    try:
        if args.cmd == "health":
            result = client.health()
        elif args.cmd == "init":
            result = client.init(db_path=args.db)
        elif args.cmd == "add":
            result = client.add(
                case=args.case,
                inp=args.inp,
                input_files=args.input_file,
                bin_name=args.bin_name,
                status=args.status,
                note=args.note,
                work_dir=args.work_dir,
                db_path=args.db,
            )
        elif args.cmd == "done":
            result = client.done(case=args.case, db_path=args.db)
        elif args.cmd == "list":
            result = client.list(db_path=args.db)
        else:
            return 1
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
