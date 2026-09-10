"""Redis bus: key naming + connections for the daemon/worker bridge.

Everything crosses ONE bus — the bench's QUEUE redis (the same instance Frappe's
realtime already uses as its python↔node bridge).  The cache redis is deliberately
avoided: ``frappe.cache`` (RedisWrapper) prefixes ``lpush``/``rpush`` keys with
``{db_name}|`` but NOT ``blpop``/``brpop``, so mixing the two on it silently never
matches.  Raw, unprefixed keys on the queue redis sidestep that entirely; keys are
site-scoped by hand instead (the queue redis is shared bench-wide).

Wire format is JSON only — never pickle — so the daemon and workers stay decoupled
from each other's python environments.
"""

from __future__ import annotations

import frappe

PREFIX = "pws"

# Reply keys are expired defensively: if the calling worker died or timed out, an
# unread reply would otherwise leak forever (the queue redis has no eviction).
REPLY_TTL_SEC = 60

# Health keys expire on their own so a dead daemon reads as unhealthy within TTL.
HEALTH_TTL_SEC = 15


def request_list_key(site: str, connection: str) -> str:
    """RPC request list for one named connection (daemon BLPOPs, workers LPUSH)."""
    return f"{PREFIX}:{site}:req:{connection}"


def reply_key(site: str, correlation_id: str) -> str:
    """Per-call reply key (daemon LPUSHes + expires, the caller BLPOPs then deletes)."""
    return f"{PREFIX}:{site}:reply:{correlation_id}"


def control_channel(site: str) -> str:
    """Pub/sub channel for control messages (resync / pause / app-defined actions)."""
    return f"{PREFIX}:{site}:ctl"


def health_key(site: str, connection: str) -> str:
    """Heartbeat key the daemon refreshes; missing/expired = connection unhealthy."""
    return f"{PREFIX}:{site}:health:{connection}"


def get_sync_redis():
    """Blocking redis client for callers running inside Frappe (workers / web).

    ``get_redis_conn`` returns the raw queue-redis client (no key prefixing) and
    honours the bench's RQ auth configuration; its connection pool is fork-safe.
    """
    from frappe.utils.background_jobs import get_redis_conn

    return get_redis_conn()


def get_async_redis():
    """Non-blocking redis client for the daemon's asyncio loop."""
    import redis.asyncio as aredis

    return aredis.from_url(frappe.conf.get("redis_queue"), decode_responses=True)
