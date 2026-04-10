"""Backward-compatible entrypoint for remote_api client."""

from remote_api.client import RemoteRequestError, RemoteResponseError, RemoteTransportError, SimDbClient, main

__all__ = [
    "RemoteRequestError",
    "RemoteTransportError",
    "RemoteResponseError",
    "SimDbClient",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
