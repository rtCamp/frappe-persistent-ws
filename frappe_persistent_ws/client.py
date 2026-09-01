"""Caller-side API — what consumer apps use from workers / web requests.

``call()`` is a synchronous request/response over the daemon's persistent socket:
LPUSH the request onto the connection's list, BLPOP the per-call reply key.  The
BLPOP timeout must stay well under the caller's RQ job timeout (RQ's SIGALRM
interrupts a blocked BLPOP, so a call can never hang past the job budget — but a
clean timeout error beats a SIGALRM kill).
"""

from __future__ import annotations

import json
from typing import Any

import frappe

from frappe_persistent_ws import bus

DEFAULT_TIMEOUT_SEC = 30


class PersistentWSError(Exception):
    """Base class for bridge errors."""


class PersistentWSUnavailable(PersistentWSError):
    """The daemon (or this connection) is down/unhealthy — callers should fail
    fast or fall back, never queue blindly."""


class PersistentWSTimeout(PersistentWSError):
    """No reply within the timeout (the request may still execute — callers must
    treat this like any network timeout: unknown outcome)."""


class PersistentWSRemoteError(PersistentWSError):
    """The connection raised while performing the request.  ``error_type`` carries
    the remote exception class name so adapters can map it back to a local type
    (e.g. a rate-limit error)."""

    def __init__(self, message: str, error_type: str = ""):
        super().__init__(message)
        self.error_type = error_type


def connection_health(connection: str, site: str | None = None) -> dict | None:
    """The connection's health payload, or ``None`` when unhealthy/absent."""
    r = bus.get_sync_redis()
    raw = r.get(bus.health_key(site or frappe.local.site, connection))
    if not raw:
        return None
    try:
        return json.loads(raw)
    except TypeError, ValueError:
        return None


def call(
    connection: str,
    endpoint: str,
    payload: Any = None,
    query: Any = None,
    timeout: int = DEFAULT_TIMEOUT_SEC,
    check_health: bool = True,
) -> Any:
    """Perform one request on a named persistent connection and return its result.

    Raises :class:`PersistentWSUnavailable` when the connection's health key is
    missing (fail fast instead of queueing onto a dead daemon),
    :class:`PersistentWSTimeout` on no reply, and :class:`PersistentWSRemoteError`
    when the connection itself raised.
    """
    site = frappe.local.site
    if check_health and connection_health(connection, site) is None:
        raise PersistentWSUnavailable(
            f"Persistent connection '{connection}' is not available (daemon down or connection offline)."
        )

    correlation_id = frappe.generate_hash(length=24)
    reply_to = bus.reply_key(site, correlation_id)
    request = {
        "endpoint": endpoint,
        "payload": payload,
        "query": query,
        "correlation_id": correlation_id,
        "reply_to": reply_to,
    }

    r = bus.get_sync_redis()
    r.lpush(bus.request_list_key(site, connection), json.dumps(request, default=str))
    popped = r.blpop(reply_to, timeout=timeout)
    if popped is None:
        # Late replies expire on their own (REPLY_TTL), but delete defensively.
        r.delete(reply_to)
        raise PersistentWSTimeout(f"No reply from persistent connection '{connection}' within {timeout}s.")

    _key, raw = popped
    envelope = json.loads(raw)
    if not envelope.get("ok"):
        raise PersistentWSRemoteError(envelope.get("error") or "unknown remote error", envelope.get("error_type") or "")
    return envelope.get("result")


def publish_control(connection: str, action: str, data: Any = None, site: str | None = None) -> None:
    """Tell the daemon something changed (e.g. re-issue a subscription after the
    account list grew).  Fire-and-forget, mirrors ``frappe.publish_realtime``."""
    r = bus.get_sync_redis()
    r.publish(
        bus.control_channel(site or frappe.local.site),
        json.dumps({"connection": connection, "action": action, "data": data}, default=str),
    )
