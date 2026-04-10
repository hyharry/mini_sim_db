"""HTTP host for mini_sim_db.

Small stdlib-only JSON API around sim_db.py for centralized case updates.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from sim_db import DEFAULT_DB_PATH, add_sim_item, init_sim_db, list_items, mark_done


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

        # safest default: only configured default DB path is writable
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
            db_path = query.get("db_path")
            try:
                resolved = self.server.policy.resolve_db_path(db_path)
                data = list_items(resolved)
            except Exception as exc:  # user-facing API error
                self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            self._json(HTTPStatus.OK, {"db_path": resolved, "cases": data})
            return

        self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path not in {"/init", "/add", "/done"}:
            self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return

        self._require_auth()
        if not self._authorized:
            return

        payload = self._read_json_body()
        if payload is None:
            return

        req_db_path = payload.get("db_path") if isinstance(payload, dict) else None
        try:
            db_path = self.server.policy.resolve_db_path(req_db_path)
        except Exception as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return

        try:
            if self.path == "/init":
                init_sim_db(db_path)
                self._json(HTTPStatus.OK, {"ok": True, "db_path": db_path})
                return

            if self.path == "/add":
                add_sim_item(
                    case=payload["case"],
                    inp=payload.get("inp"),
                    input_files=payload.get("input_files"),
                    bin_name=payload["bin_name"],
                    status=payload["status"],
                    db_path=db_path,
                    note=payload.get("note"),
                    work_dir=payload.get("work_dir"),
                )
                self._json(HTTPStatus.OK, {"ok": True, "db_path": db_path, "case": payload["case"]})
                return

            if self.path == "/done":
                mark_done(case=payload["case"], db_path=db_path)
                self._json(HTTPStatus.OK, {"ok": True, "db_path": db_path, "case": payload["case"]})
                return

        except KeyError as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": f"missing field: {exc.args[0]}"})
        except (ValueError, FileNotFoundError) as exc:
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
