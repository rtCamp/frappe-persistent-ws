"""The supervised broker daemon.

One process per site (singleton via a bench-local file lock — see ``commands.py``),
holding every registered persistent connection.  Modeled on how this stack already
runs its only trusted long-lived processes (``bench schedule`` and the realtime
node server): a plain supervised OS process that prefers crashing fast over
in-process heroics, with Redis as the only bridge to Frappe workers.

Per connection the daemon supervises four concurrent lanes:

* the **RPC pump** — BLPOP this connection's request list, ``await
  connection.request(...)``, reply via ``LPUSH + EXPIRE`` (orphan-proof);
* the **heartbeat** loop (protocol keepalive / token renewal);
* the **health** writer — a TTL'd redis key callers use to fail fast;
* the connection's optional :meth:`run` lane (event listeners).

Any lane failing tears the connection down; the supervisor closes it and
reconnects with jittered, capped backoff — deliberately gentle, because hammering
reconnects at a partner API's connection cap is itself an incident (LIVE-170).
"""

from __future__ import annotations

import asyncio
import json
import random
from typing import Any

import frappe

from frappe_persistent_ws import bus
from frappe_persistent_ws.connection import BasePersistentConnection

RECONNECT_BASE_SEC = 1.0
RECONNECT_CAP_SEC = 60.0
RPC_POP_TIMEOUT_SEC = 1  # short so shutdown stays responsive
HEALTH_REFRESH_SEC = bus.HEALTH_TTL_SEC / 3


class DaemonContext:
    """What the daemon hands to connection code (``run`` / ``handle_control``)."""

    def __init__(self, site: str, logger):
        self.site = site
        self.logger = logger

    def refresh_db(self) -> None:
        """Ensure a live DB connection — a long-lived process outlives MySQL's
        idle timeout, so connection code must call this before reading doctypes."""
        try:
            frappe.db.sql("select 1")
        except Exception:
            frappe.db.connect()

    def enqueue(self, method: str, queue: str = "default", **kwargs) -> None:
        """Hand work to a normal RQ worker (the daemon itself never mutates the DB).

        ``enqueue_after_commit`` is deliberately NOT set: the daemon runs no
        transactions and never commits, so an after-commit job would never fire.
        """
        self.refresh_db()
        # nosemgrep: frappe-semgrep.rules.frappe-enqueue-without-after-commit
        frappe.enqueue(method, queue=queue, **kwargs)


class PersistentWSDaemon:
    def __init__(self, site: str):
        self.site = site
        self.logger = frappe.logger("persistent_ws", allow_site=site)
        self.ctx = DaemonContext(site, self.logger)
        self._stop = asyncio.Event()
        self._redis = None
        self._connections: dict[str, BasePersistentConnection] = {}

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        from frappe_persistent_ws.registry import get_connections

        self._redis = bus.get_async_redis()
        try:
            connections = get_connections()
            if not connections:
                self.logger.warning("persistent_ws: no connections registered — exiting")
                return
            self._connections = {c.name: c for c in connections}
            self.logger.info(f"persistent_ws: starting with connections {sorted(self._connections)}")

            tasks = [asyncio.ensure_future(self._control_listener())]
            tasks += [asyncio.ensure_future(self._supervise(c)) for c in connections]
            await self._stop.wait()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.logger.info("persistent_ws: stopped")
        finally:
            await self._redis.aclose()

    # ------------------------------------------------------------------ #
    # per-connection supervision
    # ------------------------------------------------------------------ #

    async def _supervise(self, conn: BasePersistentConnection) -> None:
        attempt = 0
        while not self._stop.is_set():
            try:
                self.ctx.refresh_db()
                await conn.connect()
                attempt = 0
                self.logger.info(f"persistent_ws[{conn.name}]: connected")
                await self._run_lanes(conn)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception(f"persistent_ws[{conn.name}]: connection failed")
            finally:
                try:
                    await conn.close()
                except Exception:
                    self.logger.exception(f"persistent_ws[{conn.name}]: close failed")
                await self._redis.delete(bus.health_key(self.site, conn.name))

            attempt += 1
            # Jittered, capped exponential backoff — never storm the partner's
            # connection cap on a flap.
            delay = min(RECONNECT_CAP_SEC, RECONNECT_BASE_SEC * (2 ** min(attempt, 6)))
            delay *= 0.5 + random.random()  # jitter, not crypto
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:  # asyncio name: builtin alias only since 3.11 (we support 3.10)
                pass

    async def _run_lanes(self, conn: BasePersistentConnection) -> None:
        """Run the connection's lanes until the first one fails (or shutdown)."""
        lanes = [
            asyncio.ensure_future(self._rpc_pump(conn)),
            asyncio.ensure_future(self._health_writer(conn)),
            asyncio.ensure_future(conn.run(self.ctx)),
        ]
        if conn.heartbeat_interval > 0:
            lanes.append(asyncio.ensure_future(self._heartbeat(conn)))
        try:
            done, _pending = await asyncio.wait(lanes, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                exc = task.exception()
                if exc is not None:
                    raise exc
            # A lane returning cleanly (e.g. a run() that exited) still means the
            # connection should be recycled — surface it as a restart.
            raise RuntimeError("a connection lane exited")
        finally:
            for task in lanes:
                task.cancel()
            await asyncio.gather(*lanes, return_exceptions=True)

    # ------------------------------------------------------------------ #
    # lanes
    # ------------------------------------------------------------------ #

    async def _rpc_pump(self, conn: BasePersistentConnection) -> None:
        key = bus.request_list_key(self.site, conn.name)
        while True:
            popped = await self._redis.blpop(key, timeout=RPC_POP_TIMEOUT_SEC)
            if popped is None:
                continue
            _list, raw = popped
            try:
                message = json.loads(raw)
            except (TypeError, ValueError):
                self.logger.error(f"persistent_ws[{conn.name}]: dropping malformed RPC request: {raw!r}")
                continue

            reply_to = message.get("reply_to")
            try:
                result = await conn.request(
                    message.get("endpoint"), payload=message.get("payload"), query=message.get("query")
                )
                envelope: dict[str, Any] = {"ok": True, "result": result}
            except asyncio.CancelledError:
                # Shutting down / recycling mid-request: put the request back so it
                # isn't silently lost, then let the cancellation unwind.
                await self._redis.lpush(key, raw)
                raise
            except Exception as exc:
                self.logger.exception(f"persistent_ws[{conn.name}]: RPC {message.get('endpoint')} failed")
                envelope = {"ok": False, "error": str(exc), "error_type": type(exc).__name__}

            if reply_to:
                pipe = self._redis.pipeline()
                pipe.lpush(reply_to, json.dumps(envelope, default=str))
                pipe.expire(reply_to, bus.REPLY_TTL_SEC)
                await pipe.execute()

    async def _heartbeat(self, conn: BasePersistentConnection) -> None:
        while True:
            await asyncio.sleep(conn.heartbeat_interval)
            await conn.heartbeat()

    async def _health_writer(self, conn: BasePersistentConnection) -> None:
        key = bus.health_key(self.site, conn.name)
        while True:
            await self._redis.set(key, json.dumps({"ok": True}), ex=bus.HEALTH_TTL_SEC)
            await asyncio.sleep(HEALTH_REFRESH_SEC)

    # ------------------------------------------------------------------ #
    # control channel
    # ------------------------------------------------------------------ #

    async def _control_listener(self) -> None:
        """Route ``{connection, action, data}`` messages published by apps
        (``client.publish_control``) to the addressed connection.  Errors are
        logged, never fatal — a bad control message must not take the bus down."""
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(bus.control_channel(self.site))
        try:
            async for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                try:
                    body = json.loads(message["data"])
                    conn = self._connections.get(body.get("connection"))
                    if conn is None:
                        self.logger.warning(f"persistent_ws: control for unknown connection {body.get('connection')!r}")
                        continue
                    await conn.handle_control(body.get("action"), body.get("data"), self.ctx)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.logger.exception("persistent_ws: control message failed")
        finally:
            await pubsub.aclose()
