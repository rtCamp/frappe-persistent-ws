"""The connection contract consumer apps implement.

A *connection* is one named, long-lived, authenticated socket to an external
service (e.g. one Tradovate org+environment).  Consumer apps subclass
:class:`BasePersistentConnection` and register instances via the
``persistent_ws_connections`` hook (see ``registry.py``).  The daemon owns the
lifecycle — connect, supervise, reconnect with backoff, close — while the
subclass owns everything protocol-specific.

Two lanes reach a connection:

* **RPC** — workers call ``frappe_persistent_ws.client.call(name, endpoint, ...)``;
  the daemon pops the request off this connection's list and awaits
  :meth:`request`.  Exceptions raised there are serialized back to the caller.
* **Events** — a connection that listens for unsolicited server frames implements
  :meth:`run` (a long-running coroutine, e.g. a read loop dispatching events via
  ``ctx.enqueue``) — the daemon supervises it alongside the RPC pump.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from frappe_persistent_ws.daemon import DaemonContext


class BasePersistentConnection:
    #: Unique connection name — the address used by callers and control messages.
    name: str = ""

    #: Seconds between :meth:`heartbeat` calls; ``0`` disables the heartbeat task.
    heartbeat_interval: float = 0.0

    async def connect(self) -> None:
        """Open the socket and authenticate.  Raise on failure — the daemon logs,
        backs off (jittered, capped), and retries.  Called again after every drop,
        so it must also refresh credentials/tokens as needed."""
        raise NotImplementedError

    async def close(self) -> None:
        """Best-effort teardown.  Called after any failure and on shutdown; must
        never raise."""

    async def request(self, endpoint: str, payload: Any = None, query: Any = None) -> Any:
        """Perform one request/response exchange on the open socket (RPC lane).

        The return value must be JSON-serializable — it travels back to the
        calling worker over redis.  Exceptions are serialized as
        ``{ok: false, error, error_type}`` and re-raised caller-side as
        ``PersistentWSRemoteError``."""
        raise NotImplementedError

    async def heartbeat(self) -> None:
        """Periodic keepalive (e.g. send the protocol's heartbeat frame, renew an
        expiring token).  A raise here tears the connection down for a clean
        reconnect — appropriate when the socket is found dead."""

    async def run(self, ctx: DaemonContext) -> None:
        """Optional long-running lane (event listeners).  The default returns
        immediately (pure-RPC connection).  A raise or return while the daemon is
        up tears the connection down for a reconnect."""

    async def handle_control(self, action: str, data: Any, ctx: DaemonContext) -> None:
        """Handle a control message published to this connection (e.g. a resync
        request after the subscription list changed).  Exceptions are logged, not
        fatal."""
