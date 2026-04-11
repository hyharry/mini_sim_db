"""Tiny REST client for mini_sim_db server with local durable dual-write."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from typing import Any
from urllib import error, parse, request


class RemoteRequestError(RuntimeError):
    pass


class RemoteTransportError(RemoteRequestError):
    pass


class RemoteResponseError(RemoteRequestError):
    pass


from sim_db import _read_sim_db, add_sim_item, del_case_by_job_id, init_sim_db, mark_done, resolve_job_id, upd_case_by_job_id


def _case_ref(*, case: str | None, job_id: str | None) -> str:
    if bool(case) == bool(job_id):
        raise ValueError("use exactly one of case or job_id")
    return str(case or job_id)


class SimDbClient:
    def __init__(self, base_url: str, token: str, timeout: float = 10.0, local_db_path: str | None = None, enable_local_write: bool = True) -> None:
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

    def create(self, *, case: str, bin_name: str, status: str, inp: str | None = None, input_files: list[str] | None = None, note: str | None = None, work_dir: str | None = None, extra_params: str | None = None, db_path: str | None = None, run_host: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"case": case, "bin_name": bin_name, "status": status, "run_host": run_host or socket.gethostname()}
        if inp is not None:
            payload["inp"] = inp
        if input_files is not None:
            payload["input_files"] = input_files
        if note is not None:
            payload["note"] = note
        if work_dir is not None:
            payload["work_dir"] = work_dir
        if extra_params is not None:
            payload["extra_params"] = extra_params
        if db_path is not None:
            payload["db_path"] = db_path
        return self._dual_write("create", payload)

    def add(self, **kwargs: Any) -> dict[str, Any]:
        return self.create(**kwargs)

    def read(self, *, case: str | None = None, job_id: str | None = None, db_path: str | None = None) -> dict[str, Any]:
        case_ref = _case_ref(case=case, job_id=job_id)
        path = f"/cases/{parse.quote(case_ref, safe='')}"
        if db_path:
            path += "?" + parse.urlencode({"db_path": db_path})
        return self._request("GET", path)

    def done(self, *, case: str | None = None, job_id: str | None = None, db_path: str | None = None, run_host: str | None = None) -> dict[str, Any]:
        return self.update(case=case, job_id=job_id, fields={"status": "done"}, db_path=db_path, run_host=run_host)

    def update(self, *, case: str | None = None, job_id: str | None = None, fields: dict[str, Any], db_path: str | None = None, run_host: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"case": case, "job_id": job_id, "fields": fields, "run_host": run_host or socket.gethostname()}
        if db_path is not None:
            payload["db_path"] = db_path
        return self._dual_write("update", payload)

    def delete(self, *, case: str | None = None, job_id: str | None = None, db_path: str | None = None, run_host: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"case": case, "job_id": job_id, "run_host": run_host or socket.gethostname()}
        if db_path is not None:
            payload["db_path"] = db_path
        return self._dual_write("delete", payload)

    def list(self, db_path: str | None = None) -> dict[str, Any]:
        path = "/cases"
        if db_path:
            path += "?" + parse.urlencode({"db_path": db_path})
        return self._request("GET", path)

    def summary(self, *, db_path: str | None = None, status: str | None = None, run_host: str | None = None, limit: int | None = None, sort_by: str = "updated_at", order: str = "desc") -> dict[str, Any]:
        query: dict[str, Any] = {"sort_by": sort_by, "order": order}
        if db_path:
            query["db_path"] = db_path
        if status:
            query["status"] = status
        if run_host:
            query["run_host"] = run_host
        if limit is not None:
            query["limit"] = limit
        return self._request("GET", "/cases/summary?" + parse.urlencode(query))

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
        except RemoteTransportError as exc:
            if local_ok:
                return {"ok": True, "remote_ok": False, "remote_error": str(exc), "local_ok": True, "fallback": "local-only"}
            raise

    def _apply_local(self, op: str, payload: dict[str, Any]) -> None:
        assert self.local_db_path is not None
        init_sim_db(self.local_db_path)
        run_host = payload.get("run_host")
        if op == "create":
            if payload.get("job_id") not in (None, ""):
                raise ValueError("field 'job_id' is auto-generated and cannot be set on create")
            add_sim_item(case=payload["case"], inp=payload.get("inp"), input_files=payload.get("input_files"), bin_name=payload["bin_name"], status=payload["status"], db_path=self.local_db_path, note=payload.get("note"), work_dir=payload.get("work_dir"), extra_params=payload.get("extra_params"))
            _, rows = _read_sim_db(self.local_db_path)
            job_id = resolve_job_id(rows, case=payload["case"])
            if run_host:
                upd_case_by_job_id(self.local_db_path, job_id, {"run_host": str(run_host)})
            return
        if op == "update":
            _, rows = _read_sim_db(self.local_db_path)
            job_id = resolve_job_id(rows, case=payload.get("case"), job_id=payload.get("job_id"))
            fields = dict(payload.get("fields") or {})
            if fields.get("status") == "done":
                mark_done(job_id=job_id, db_path=self.local_db_path)
                fields.pop("status", None)
            if run_host:
                fields["run_host"] = str(run_host)
            if fields:
                upd_case_by_job_id(self.local_db_path, job_id, fields)
            return
        if op == "delete":
            _, rows = _read_sim_db(self.local_db_path)
            job_id = resolve_job_id(rows, case=payload.get("case"), job_id=payload.get("job_id"))
            del_case_by_job_id(self.local_db_path, job_id)
            return
        raise ValueError(f"unsupported op: {op}")

    def _request_for_op(self, op: str, payload: dict[str, Any]) -> dict[str, Any]:
        if op == "create":
            return self._request("POST", "/cases", payload)
        if op == "update":
            case_ref = parse.quote(_case_ref(case=payload.get("case"), job_id=payload.get("job_id")), safe='')
            remote_payload = {k: v for k, v in payload.items() if k not in {"case", "job_id"}}
            return self._request("PATCH", f"/cases/{case_ref}", remote_payload)
        if op == "delete":
            case_ref = parse.quote(_case_ref(case=payload.get("case"), job_id=payload.get("job_id")), safe='')
            db_path = payload.get("db_path")
            path = f"/cases/{case_ref}"
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
            raise RemoteResponseError(f"HTTP {exc.code}: {body or str(exc)}") from exc
        except (error.URLError, TimeoutError) as exc:
            raise RemoteTransportError(f"request failed: {exc}") from exc


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
    parser.add_argument("--extra-params", default=None)
    parser.add_argument("--db", default=None)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="mini_sim_db REST client")
    p.add_argument("--url", default="http://127.0.0.1:8765", help="Server base URL")
    p.add_argument("--token", default=None, help="Bearer token (or SIM_DB_API_TOKEN)")
    p.add_argument("--local-db", default=os.path.expanduser("~/.sim_db_client_local.sqlite3"), help="Local durable mirror DB for dual-write fallback")
    p.add_argument("--no-local-write", action="store_true", help="Disable local dual-write fallback")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("health")
    p_init = sub.add_parser("init")
    p_init.add_argument("--db", default=None)
    p_create = sub.add_parser("create")
    _add_create_like_args(p_create)
    p_add = sub.add_parser("add")
    _add_create_like_args(p_add)
    p_read = sub.add_parser("read")
    g = p_read.add_mutually_exclusive_group(required=True)
    g.add_argument("--case")
    g.add_argument("--job-id", dest="job_id")
    p_read.add_argument("--db", default=None)
    p_update = sub.add_parser("update")
    g = p_update.add_mutually_exclusive_group(required=True)
    g.add_argument("--case")
    g.add_argument("--job-id", dest="job_id")
    p_update.add_argument("--field", action="append", default=None)
    p_update.add_argument("--db", default=None)
    p_done = sub.add_parser("done")
    g = p_done.add_mutually_exclusive_group(required=True)
    g.add_argument("--case")
    g.add_argument("--job-id", dest="job_id")
    p_done.add_argument("--db", default=None)
    p_delete = sub.add_parser("delete")
    g = p_delete.add_mutually_exclusive_group(required=True)
    g.add_argument("--case")
    g.add_argument("--job-id", dest="job_id")
    p_delete.add_argument("--db", default=None)
    p_list = sub.add_parser("list")
    p_list.add_argument("--db", default=None)
    p_summary = sub.add_parser("summary")
    p_summary.add_argument("--db", default=None)
    p_summary.add_argument("--status", default=None)
    p_summary.add_argument("--run-host", default=None)
    p_summary.add_argument("--limit", type=int, default=None)
    p_summary.add_argument("--sort-by", default="updated_at")
    p_summary.add_argument("--order", choices=["asc", "desc"], default="desc")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    token = args.token or os.getenv("SIM_DB_API_TOKEN")
    if not token:
        print("Missing token. Set --token or SIM_DB_API_TOKEN", file=sys.stderr)
        return 2
    client = SimDbClient(base_url=args.url, token=token, local_db_path=None if args.no_local_write else args.local_db, enable_local_write=not args.no_local_write)
    try:
        if args.cmd == "health":
            result = client.health()
        elif args.cmd == "init":
            result = client.init(db_path=args.db)
        elif args.cmd in {"create", "add"}:
            result = client.create(case=args.case, inp=args.inp, input_files=args.input_file, bin_name=args.bin_name, status=args.status, note=args.note, work_dir=args.work_dir, extra_params=args.extra_params, db_path=args.db)
        elif args.cmd == "read":
            result = client.read(case=args.case, job_id=args.job_id, db_path=args.db)
        elif args.cmd == "update":
            result = client.update(case=args.case, job_id=args.job_id, fields=_parse_fields(args.field), db_path=args.db)
        elif args.cmd == "done":
            result = client.done(case=args.case, job_id=args.job_id, db_path=args.db)
        elif args.cmd == "delete":
            result = client.delete(case=args.case, job_id=args.job_id, db_path=args.db)
        elif args.cmd == "list":
            result = client.list(db_path=args.db)
        else:
            result = client.summary(db_path=args.db, status=args.status, run_host=args.run_host, limit=args.limit, sort_by=args.sort_by, order=args.order)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
