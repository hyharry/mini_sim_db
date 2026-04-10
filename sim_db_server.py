"""HTTP host for mini_sim_db.

Stdlib-only JSON API around sim_db.py for centralized CRUD updates.
"""

from __future__ import annotations

import argparse
import json
import os
import threading
from datetime import datetime
from pathlib import Path
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from sim_db import (
    ALLOWED_STATUS,
    DEFAULT_DB_PATH,
    add_sim_item,
    del_cases,
    init_sim_db,
    list_items,
    upd_cases,
)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


class SecurityPolicy:
    """Validates API token and requested DB paths."""

    def __init__(
        self,
        token: str,
        default_db_path: str,
        allowed_db_path: str | None = None,
        allowed_base_dir: str | None = None,
    ) -> None:
        self.token = token
        self.default_db_path = _norm(default_db_path)
        self.allowed_db_path = _norm(allowed_db_path) if allowed_db_path else None
        self.allowed_base_dir = _norm(allowed_base_dir) if allowed_base_dir else None

    def is_authorized(self, auth_header: str | None) -> bool:
        if not auth_header or not auth_header.startswith("Bearer "):
            return False
        return auth_header[len("Bearer ") :] == self.token

    def resolve_db_path(self, requested_db_path: str | None) -> str:
        if not requested_db_path:
            return self.default_db_path

        wanted = _norm(requested_db_path)

        if self.allowed_db_path is None and self.allowed_base_dir is None:
            if wanted == self.default_db_path:
                return wanted
            raise ValueError("db_path is not allowed")

        if self.allowed_db_path and wanted == self.allowed_db_path:
            return wanted

        if self.allowed_base_dir and _is_within_base(wanted, self.allowed_base_dir):
            return wanted

        raise ValueError("db_path is outside allowed scope")


class SimDbApiServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], policy: SecurityPolicy) -> None:
        self.policy = policy
        self.mutation_lock = threading.Lock()
        super().__init__(server_address, SimDbRequestHandler)


class SimDbRequestHandler(BaseHTTPRequestHandler):
    server: SimDbApiServer

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._json(HTTPStatus.OK, {"ok": True})
            return

        if self.path.startswith("/cases"):
            self._require_auth()
            if not self._authorized:
                return

            query = self._query_dict()
            try:
                db_path = self.server.policy.resolve_db_path(query.get("db_path"))
                case_ref = self._case_from_path()
                with self.server.mutation_lock:
                    data = list_items(db_path)
                    if case_ref:
                        case = self._resolve_case_ref_from_table(data, case_ref)
                        self._json(HTTPStatus.OK, {"db_path": db_path, "case": case, "item": data[case]})
                        return
                self._json(HTTPStatus.OK, {"db_path": db_path, "cases": data})
            except Exception as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return

        self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/init":
            self._require_auth()
            if not self._authorized:
                return
            payload = self._read_json_body()
            if payload is None:
                return
            try:
                db_path = self.server.policy.resolve_db_path(payload.get("db_path"))
                with self.server.mutation_lock:
                    init_sim_db(db_path)
                self._json(HTTPStatus.OK, {"ok": True, "db_path": db_path})
            except Exception as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return

        if self.path in {"/add", "/done", "/update", "/delete"}:
            self._legacy_mutating_routes()
            return

        if self.path == "/cases":
            self._require_auth()
            if not self._authorized:
                return
            payload = self._read_json_body()
            if payload is None:
                return
            self._create_case(payload)
            return

        self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_PATCH(self) -> None:  # noqa: N802
        if not self.path.startswith("/cases/"):
            self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        self._require_auth()
        if not self._authorized:
            return

        payload = self._read_json_body()
        if payload is None:
            return

        case = self._case_from_path()
        if not case:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "missing case in path"})
            return

        self._update_case(case, payload)

    def do_DELETE(self) -> None:  # noqa: N802
        if not self.path.startswith("/cases/"):
            self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        self._require_auth()
        if not self._authorized:
            return

        case_ref = self._case_from_path()
        if not case_ref:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "missing case in path"})
            return

        query = self._query_dict()
        req_db = query.get("db_path")
        try:
            db_path = self.server.policy.resolve_db_path(req_db)
            with self.server.mutation_lock:
                table = list_items(db_path)
                case = self._resolve_case_ref_from_table(table, case_ref)
                del_cases(db_path, [case])
            self._json(HTTPStatus.OK, {"ok": True, "db_path": db_path, "case": case})
        except Exception as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def _legacy_mutating_routes(self) -> None:
        self._require_auth()
        if not self._authorized:
            return
        payload = self._read_json_body()
        if payload is None:
            return

        if self.path == "/add":
            self._create_case(payload)
            return
        if self.path == "/done":
            case_ref = payload.get("case") or payload.get("job_id")
            if not case_ref:
                self._json(HTTPStatus.BAD_REQUEST, {"error": "missing field: case or job_id"})
                return
            self._update_case(case_ref, {"fields": {"status": "done"}, **payload})
            return
        if self.path == "/update":
            case_ref = payload.get("case") or payload.get("job_id")
            if not case_ref:
                self._json(HTTPStatus.BAD_REQUEST, {"error": "missing field: case or job_id"})
                return
            self._update_case(case_ref, payload)
            return
        if self.path == "/delete":
            case_ref = payload.get("case") or payload.get("job_id")
            if not case_ref:
                self._json(HTTPStatus.BAD_REQUEST, {"error": "missing field: case or job_id"})
                return
            try:
                db_path = self.server.policy.resolve_db_path(payload.get("db_path"))
                with self.server.mutation_lock:
                    table = list_items(db_path)
                    case = self._resolve_case_ref_from_table(table, case_ref)
                    del_cases(db_path, [case])
                self._json(HTTPStatus.OK, {"ok": True, "db_path": db_path, "case": case})
            except Exception as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def _create_case(self, payload: dict[str, Any]) -> None:
        try:
            if payload.get("job_id") not in (None, ""):
                raise ValueError("field 'job_id' is auto-generated and cannot be set on create")

            db_path = self.server.policy.resolve_db_path(payload.get("db_path"))
            with self.server.mutation_lock:
                add_sim_item(
                    case=payload["case"],
                    inp=payload.get("inp"),
                    input_files=payload.get("input_files"),
                    bin_name=payload["bin_name"],
                    status=payload["status"],
                    db_path=db_path,
                    note=payload.get("note"),
                    work_dir=payload.get("work_dir"),
                    extra_params=payload.get("extra_params"),
                )
                run_host = payload.get("run_host")
                if run_host:
                    upd_cases(db_path, {payload["case"]: {"run_host": str(run_host)}})
            self._json(HTTPStatus.OK, {"ok": True, "db_path": db_path, "case": payload["case"]})
        except KeyError as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": f"missing field: {exc.args[0]}"})
        except Exception as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    @staticmethod
    def _resolve_case_ref_from_table(data: dict[str, dict[str, Any]], case_ref: str) -> str:
        if case_ref in data:
            return case_ref

        matches = [case for case, item in data.items() if item.get("job_id") == case_ref]
        if not matches:
            raise ValueError(f"case/job_id not found: {case_ref}")
        if len(matches) > 1:
            joined = ", ".join(sorted(matches))
            raise ValueError(f"job_id matches multiple cases ({joined}), use case explicitly")
        return matches[0]

    def _update_case(self, case_ref: str, payload: dict[str, Any]) -> None:
        try:
            db_path = self.server.policy.resolve_db_path(payload.get("db_path"))
            fields = payload.get("fields")
            if fields is None:
                fields = {k: v for k, v in payload.items() if k not in {"case", "job_id", "db_path", "run_host"}}
            if not isinstance(fields, dict) or not fields:
                raise ValueError("fields must be a non-empty object")
            if "case" in fields:
                raise ValueError("field 'case' is immutable")

            status = fields.get("status")
            if status is not None:
                if status not in ALLOWED_STATUS:
                    allowed = ", ".join(sorted(ALLOWED_STATUS))
                    raise ValueError(f"Invalid status '{status}'. Allowed: {allowed}")
                fields["state_changed_at"] = _now_iso()

            fields["updated_at"] = _now_iso()

            run_host = payload.get("run_host")
            if run_host:
                fields["run_host"] = str(run_host)

            with self.server.mutation_lock:
                table = list_items(db_path)
                case = self._resolve_case_ref_from_table(table, case_ref)
                upd_cases(db_path, {case: fields})
            self._json(HTTPStatus.OK, {"ok": True, "db_path": db_path, "case": case, "updated": sorted(fields.keys())})
        except Exception as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def _read_json_body(self) -> dict[str, Any] | None:
        try:
            raw_len = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(raw_len) if raw_len > 0 else b"{}"
            payload = json.loads(raw.decode("utf-8")) if raw else {}
            if not isinstance(payload, dict):
                raise ValueError("JSON body must be an object")
            return payload
        except Exception as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": f"invalid JSON: {exc}"})
            return None

    def _query_dict(self) -> dict[str, str]:
        query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
        out: dict[str, str] = {}
        for k, vals in query.items():
            if vals:
                out[k] = vals[0]
        return out

    def _case_from_path(self) -> str | None:
        path = urlsplit(self.path).path
        parts = [p for p in path.split("/") if p]
        if len(parts) >= 2 and parts[0] == "cases":
            return unquote(parts[1])
        return None

    def _require_auth(self) -> None:
        self._authorized = self.server.policy.is_authorized(self.headers.get("Authorization"))
        if not self._authorized:
            self._json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})

    def _json(self, status: HTTPStatus, obj: dict[str, Any]) -> None:
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def _norm(path: str) -> str:
    return str(Path(path).expanduser().resolve())


def _is_within_base(path: str, base_dir: str) -> bool:
    target = Path(path).expanduser().resolve()
    base = Path(base_dir).expanduser().resolve()
    try:
        target.relative_to(base)
        return True
    except ValueError:
        return False


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="mini_sim_db HTTP server")
    p.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    p.add_argument("--port", type=int, default=8765, help="Bind port (default: 8765)")
    p.add_argument("--db", default=DEFAULT_DB_PATH, help="Default DB path (default: ~/sim_db.csv)")
    p.add_argument("--allowed-db-path", default=None, help="Optional exact writable DB path")
    p.add_argument("--allowed-base-dir", default=None, help="Optional writable base directory")
    p.add_argument("--token", default=None, help="Bearer token (or use SIM_DB_API_TOKEN env)")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    token = args.token or os.getenv("SIM_DB_API_TOKEN")
    if not token:
        raise SystemExit("Missing token. Set --token or SIM_DB_API_TOKEN")

    policy = SecurityPolicy(
        token=token,
        default_db_path=args.db,
        allowed_db_path=args.allowed_db_path,
        allowed_base_dir=args.allowed_base_dir,
    )
    server = SimDbApiServer((args.host, args.port), policy)
    print(f"Serving mini_sim_db API at http://{args.host}:{args.port}")
    print(f"Default DB path: {policy.default_db_path}")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
