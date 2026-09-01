"""Bridge tests — run against the bench's real queue redis (no external sockets).

bench --site <site> run-tests --app frappe_persistent_ws
"""

from __future__ import annotations

import asyncio
import json
import threading
from unittest.mock import patch

import frappe
from frappe.tests import IntegrationTestCase

from frappe_persistent_ws import bus, client
from frappe_persistent_ws.connection import BasePersistentConnection
from frappe_persistent_ws.daemon import PersistentWSDaemon


class EchoConnection(BasePersistentConnection):
    name = "test-echo"

    async def connect(self):
        pass

    async def request(self, endpoint, payload=None, query=None):
        if endpoint == "boom":
            raise ValueError("remote exploded")
        return {"endpoint": endpoint, "payload": payload, "query": query}


def echo_factory():
    return EchoConnection()


def echo_list_factory():
    return [EchoConnection()]


class BridgeTestCase(IntegrationTestCase):
    def setUp(self):
        self.site = frappe.local.site
        self.r = bus.get_sync_redis()
        self._cleanup_keys()

    def tearDown(self):
        self._cleanup_keys()

    def _cleanup_keys(self):
        keys = list(self.r.scan_iter(f"{bus.PREFIX}:{self.site}:*"))
        if keys:
            self.r.delete(*keys)


class TestKeyNaming(BridgeTestCase):
    def test_keys_are_site_scoped(self):
        self.assertEqual(bus.request_list_key("s1", "c1"), "pws:s1:req:c1")
        self.assertEqual(bus.reply_key("s1", "abc"), "pws:s1:reply:abc")
        self.assertEqual(bus.control_channel("s1"), "pws:s1:ctl")
        self.assertEqual(bus.health_key("s1", "c1"), "pws:s1:health:c1")


class TestRegistry(BridgeTestCase):
    HOOK_BASE = "frappe_persistent_ws.tests.test_bridge"

    def test_factories_may_return_one_or_many(self):
        from frappe_persistent_ws import registry

        with patch.object(frappe, "get_hooks", return_value=[f"{self.HOOK_BASE}.echo_factory"]):
            conns = registry.get_connections()
        self.assertEqual([c.name for c in conns], ["test-echo"])

        with patch.object(frappe, "get_hooks", return_value=[f"{self.HOOK_BASE}.echo_list_factory"]):
            conns = registry.get_connections()
        self.assertEqual([c.name for c in conns], ["test-echo"])

    def test_duplicate_names_rejected(self):
        from frappe_persistent_ws import registry

        with patch.object(
            frappe,
            "get_hooks",
            return_value=[f"{self.HOOK_BASE}.echo_factory", f"{self.HOOK_BASE}.echo_factory"],
        ):
            with self.assertRaises(frappe.ValidationError):
                registry.get_connections()


class TestRpcPump(BridgeTestCase):
    """Drive the daemon's RPC pump directly against real redis."""

    def _pump_one(self, request: dict, conn=None) -> dict:
        conn = conn or EchoConnection()
        self.r.lpush(bus.request_list_key(self.site, conn.name), json.dumps(request))

        async def run():
            daemon = PersistentWSDaemon(self.site)
            daemon._redis = bus.get_async_redis()
            try:
                # The pump loops forever; give it enough time to process one
                # message, then cancel (wait_for cancels the awaited task).
                await asyncio.wait_for(daemon._rpc_pump(conn), timeout=2)
            except TimeoutError:
                pass
            finally:
                await daemon._redis.aclose()

        asyncio.run(run())
        popped = self.r.blpop(request["reply_to"], timeout=2)
        self.assertIsNotNone(popped, "pump produced no reply")
        return json.loads(popped[1])

    def test_pump_replies_with_result_and_ttl(self):
        reply_to = bus.reply_key(self.site, "corr1")
        envelope = self._pump_one({"endpoint": "ping", "payload": {"a": 1}, "query": None, "reply_to": reply_to})
        self.assertTrue(envelope["ok"])
        self.assertEqual(envelope["result"], {"endpoint": "ping", "payload": {"a": 1}, "query": None})

    def test_pump_serializes_remote_errors(self):
        reply_to = bus.reply_key(self.site, "corr2")
        envelope = self._pump_one({"endpoint": "boom", "payload": None, "query": None, "reply_to": reply_to})
        self.assertFalse(envelope["ok"])
        self.assertEqual(envelope["error_type"], "ValueError")
        self.assertIn("remote exploded", envelope["error"])


class _FakeDaemonThread(threading.Thread):
    """A 10-line stand-in for the daemon: BRPOP one request, reply per protocol.

    The redis client is passed in — ``frappe.local`` is thread-local, so
    ``bus.get_sync_redis()`` cannot be called from inside the thread.
    """

    def __init__(self, site, connection_name, redis_client):
        super().__init__(daemon=True)
        self.site = site
        self.connection_name = connection_name
        self.redis_client = redis_client

    def run(self):
        r = self.redis_client
        popped = r.brpop(bus.request_list_key(self.site, self.connection_name), timeout=5)
        if popped is None:
            return
        request = json.loads(popped[1])
        if request["endpoint"] == "boom":
            envelope = {"ok": False, "error": "remote exploded", "error_type": "ValueError"}
        else:
            envelope = {"ok": True, "result": {"echo": request["payload"]}}
        pipe = r.pipeline()
        pipe.lpush(request["reply_to"], json.dumps(envelope))
        pipe.expire(request["reply_to"], bus.REPLY_TTL_SEC)
        pipe.execute()


class TestClientCall(BridgeTestCase):
    def test_call_round_trip(self):
        _FakeDaemonThread(self.site, "test-echo", self.r).start()
        result = client.call("test-echo", "ping", payload={"x": 2}, timeout=5, check_health=False)
        self.assertEqual(result, {"echo": {"x": 2}})

    def test_call_maps_remote_errors(self):
        _FakeDaemonThread(self.site, "test-echo", self.r).start()
        with self.assertRaises(client.PersistentWSRemoteError) as caught:
            client.call("test-echo", "boom", timeout=5, check_health=False)
        self.assertEqual(caught.exception.error_type, "ValueError")

    def test_call_fails_fast_when_unhealthy(self):
        # No health key → refuse to enqueue onto a dead daemon.
        with self.assertRaises(client.PersistentWSUnavailable):
            client.call("test-echo", "ping", timeout=5, check_health=True)

    def test_call_health_gate_passes_when_key_present(self):
        self.r.set(bus.health_key(self.site, "test-echo"), json.dumps({"ok": True}), ex=bus.HEALTH_TTL_SEC)
        _FakeDaemonThread(self.site, "test-echo", self.r).start()
        result = client.call("test-echo", "ping", payload=1, timeout=5)
        self.assertEqual(result, {"echo": 1})

    def test_call_timeout_when_no_daemon(self):
        with self.assertRaises(client.PersistentWSTimeout):
            client.call("test-echo", "ping", timeout=1, check_health=False)

    def test_publish_control_reaches_channel(self):
        pubsub = self.r.pubsub()
        pubsub.subscribe(bus.control_channel(self.site))
        try:
            pubsub.get_message(timeout=1)  # consume the subscribe ack
            client.publish_control("test-echo", "resync", data={"n": 1})
            message = pubsub.get_message(timeout=2)
            self.assertIsNotNone(message)
            body = json.loads(message["data"])
            self.assertEqual(body, {"connection": "test-echo", "action": "resync", "data": {"n": 1}})
        finally:
            pubsub.close()
