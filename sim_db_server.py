"""Backward-compatible entrypoint for remote_api server."""

from remote_api.server import SecurityPolicy, SimDbApiServer, SimDbRequestHandler, main

__all__ = ["SecurityPolicy", "SimDbApiServer", "SimDbRequestHandler", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
