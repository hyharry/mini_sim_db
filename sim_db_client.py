"""Tiny REST client for mini_sim_db server with local durable dual-write."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from typing import Any
from urllib import error, parse, request

from sim_db import add_sim_item, del_cases, init_sim_db, mark_done, upd_cases


class SimDbClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        timeout: float = 10.0,
        local_db_path: str | None = None,
        enable_local_write: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.local_db_path = os.path.expanduser(local_db_path) if local_db_path else None
        self.enable_local_write = enable_local_write

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/health")

    def init(self, db_path: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if db_path:
            payload["db_path"] = db_path
        return self._request("POST", "/init", payload)

    def create(
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
        run_host: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "case": case,
            "bin_name": bin_name,
            "status": status,
            "run_host": run_host or socket.gethostname(),
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
        return self._dual_write("create", payload)

    def add(self, **kwargs: Any) -> dict[str, Any]:
        return self.create(**kwargs)

    def read(self, *, case: str, db_path: str | None = None) -> dict[str, Any]:
        path = f"/cases/{parse.quote(case)}"
        if db_path:
            path += "?" + parse.urlencode({"db_path": db_path})
        return self._request("GET", path)

    def done(self, *, case: str, db_path: str | None = None, run_host: str | None = None) -> dict[str, Any]:
        return self.update(
            case=case,
            fields={"status": "done"},
            db_path=db_path,
            run_host=run_host,
        )

    def update(
        self,
        *,
        case: str,
        fields: dict[str, Any],
        db_path: str | None = None,
        run_host: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "case": case,
            "fields": fields,
            "run_host": run_host or socket.gethostname(),
        }
        if db_path is not None:
            payload["db_path"] = db_path
        return self._dual_write("update", payload)

    def delete(self, *, case: str, db_path: str | None = None, run_host: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "case": case,
            "run_host": run_host or socket.gethostname(),
        }
        if db_path is not None:
            payload["db_path"] = db_path
        return self._dual_write("delete", payload)

    def list(self, db_path: str | None = None) -> dict[str, Any]:
        path = "/cases"
        if db_path:
            path += "?" + parse.urlencode({"db_path": db_path})
        return self._request("GET", path)

    def _dual_write(self, op: str, payload: dict[str, Any]) -> dict[str, Any]:
        local_ok = None
        local_error = None
        if self.enable_local_write and self.local_db_path:
            try:
                self._apply_local(op, payload)
                local_ok = True
            except Exception as exc:
                local_ok = False
                local_error = str(exc)

        try:
            remote = self._request_for_op(op, payload)
            out = {"ok": True, "remote_ok": True, "remote": remote}
            if local_ok is not None:
                out["local_ok"] = local_ok
            if local_error:
                out["local_error"] = local_error
            return out
        except RuntimeError as exc:
            if local_ok:
                return {
                    "ok": True,
                    "remote_ok": False,
                    "remote_error": str(exc),
                    "local_ok": True,
                    "fallback": "local-only",
                }
            raise

    def _apply_local(self, op: str, payload: dict[str, Any]) -> None:
        assert self.local_db_path is not None
        init_sim_db(self.local_db_path)
        case = payload["case"]
        run_host = payload.get("run_host")

        if op == "create":
            add_sim_item(
                case=case,
                inp=payload.get("inp"),
                input_files=payload.get("input_files"),
                bin_name=payload["bin_name"],
                status=payload["status"],
                db_path=self.local_db_path,
                note=payload.get("note"),
                work_dir=payload.get("work_dir"),
            )
            if run_host:
                upd_cases(self.local_db_path, {case: {"run_host": str(run_host)}})
            return

        if op == "update":
            fields = dict(payload.get("fields") or {})
            if run_host:
                fields["run_host"] = str(run_host)
            if fields.get("status") == "done":
                mark_done(case=case, db_path=self.local_db_path)
                fields.pop("status", None)
                fields.pop("state_changed_at", None)
            if fields:
                upd_cases(self.local_db_path, {case: fields})
            return

        if op == "delete":
            del_cases(self.local_db_path, [case])
            return

        raise ValueError(f"unsupported op: {op}")

    def _request_for_op(self, op: str, payload: dict[str, Any]) -> dict[str, Any]:
        if op == "create":
            return self._request("POST", "/cases", payload)
        if op == "update":
            case = parse.quote(payload["case"])
            remote_payload = {k: v for k, v in payload.items() if k != "case"}
            return self._request("PATCH", f"/cases/{case}", remote_payload)
        if op == "delete":
            case = parse.quote(payload["case"])
            db_path = payload.get("db_path")
            path = f"/cases/{case}"
            if db_path:
                path += "?" + parse.urlencode({"db_path": db_path})
            return self._request("DELETE", path)
        raise ValueError(f"unsupported op: {op}")

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


def _parse_fields(pairs: list[str] | None) -> dict[str, str]:
    fields: dict[str, str] = {}
    for pair in pairs or []:
        if "=" not in pair:
            raise ValueError(f"invalid --field '{pair}', expected key=value")
        key, value = pair.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"invalid --field '{pair}', empty key")
        fields[key] = value
    return fields


def _add_create_like_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--case", required=True)
    parser.add_argument("--bin", dest="bin_name", required=True)
    parser.add_argument("--status", required=True)
    parser.add_argument("--inp", default=None)
    parser.add_argument("--input-file", action="append", default=None)
    parser.add_argument("--note", default=None)
    parser.add_argument("--work-dir", default=None)
    parser.add_argument("--db", default=None)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="mini_sim_db REST client")
    p.add_argument("--url", default="http://127.0.0.1:8765", help="Server base URL")
    p.add_argument("--token", default=None, help="Bearer token (or SIM_DB_API_TOKEN)")
    p.add_argument(
        "--local-db",
        default=os.path.expanduser("~/.sim_db_client_local.csv"),
        help="Local durable mirror DB for dual-write fallback (default: ~/.sim_db_client_local.csv)",
    )
    p.add_argument("--no-local-write", action="store_true", help="Disable local dual-write fallback")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("health")

    p_init = sub.add_parser("init")
    p_init.add_argument("--db", default=None, help="Optional remote db_path override")

    p_create = sub.add_parser("create")
    _add_create_like_args(p_create)

    p_add = sub.add_parser("add")
    _add_create_like_args(p_add)

    p_read = sub.add_parser("read")
    p_read.add_argument("--case", required=True)
    p_read.add_argument("--db", default=None)

    p_update = sub.add_parser("update")
    p_update.add_argument("--case", required=True)
    p_update.add_argument("--field", action="append", default=None, help="key=value (repeatable)")
    p_update.add_argument("--db", default=None)

    p_done = sub.add_parser("done")
    p_done.add_argument("--case", required=True)
    p_done.add_argument("--db", default=None)

    p_delete = sub.add_parser("delete")
    p_delete.add_argument("--case", required=True)
    p_delete.add_argument("--db", default=None)

    p_list = sub.add_parser("list")
    p_list.add_argument("--db", default=None)

    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    token = args.token or os.getenv("SIM_DB_API_TOKEN")
    if not token:
        print("Missing token. Set --token or SIM_DB_API_TOKEN", file=sys.stderr)
        return 2

    client = SimDbClient(
        base_url=args.url,
        token=token,
        local_db_path=None if args.no_local_write else args.local_db,
        enable_local_write=not args.no_local_write,
    )

    try:
        if args.cmd == "health":
            result = client.health()
        elif args.cmd == "init":
            result = client.init(db_path=args.db)
        elif args.cmd in {"create", "add"}:
            result = client.create(
                case=args.case,
                inp=args.inp,
                input_files=args.input_file,
                bin_name=args.bin_name,
                status=args.status,
                note=args.note,
                work_dir=args.work_dir,
                db_path=args.db,
            )
        elif args.cmd == "read":
            result = client.read(case=args.case, db_path=args.db)
        elif args.cmd == "update":
            result = client.update(case=args.case, fields=_parse_fields(args.field), db_path=args.db)
        elif args.cmd == "done":
            result = client.done(case=args.case, db_path=args.db)
        elif args.cmd == "delete":
            result = client.delete(case=args.case, db_path=args.db)
        elif args.cmd == "list":
            result = client.list(db_path=args.db)
        else:
            return 1
    except (RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
