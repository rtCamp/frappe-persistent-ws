# Frappe Persistent WS

Persistent **outbound** WebSocket connections for Frappe apps.

Frappe's own realtime system keeps *inbound* browser sockets alive in a dedicated
supervised process bridged to Python through Redis. This app is that pattern pointed
the other way: a supervised **broker daemon** that owns long-lived, authenticated
WebSocket connections to external services (trading platforms, exchanges, any
streaming API), so your workers never open sockets of their own.

## Why

Opening a fresh WebSocket per operation is slow (a connect + auth round-trip on
every call) and dangerous against providers that cap connections per partner —
enough concurrent handshakes and the provider starts rejecting you. And an event
*listener* (server-pushed frames) is impossible without a socket that outlives the
request. This app gives every Frappe app on a site:

- **Named persistent connections**, declared via a hook, each with its own
  credentials, heartbeat, and reconnect policy — run by one daemon per site.
- **An RPC lane** — workers call `client.call(connection, endpoint, payload)` and
  get the response over the shared socket (Redis list request/reply,
  correlation-id matched, orphan-proof TTL'd replies).
- **An event lane** — a connection may run a long-lived read loop and hand
  server-pushed events to normal RQ workers via `frappe.enqueue`.
- **A control channel** — apps publish `resync`/`pause`/custom actions to a
  connection at runtime (mirrors `frappe.publish_realtime`).
- **Fail-fast health** — a TTL'd health key per connection; callers get an
  immediate "bridge down" error instead of hanging.

## Usage

Declare a connection in your app's `hooks.py`:

```python
persistent_ws_connections = ["my_app.integrations.bridge.get_connections"]
```

```python
from frappe_persistent_ws.connection import BasePersistentConnection

class MyServiceConnection(BasePersistentConnection):
    name = "my-service"
    heartbeat_interval = 2.5

    async def connect(self):        ...  # open socket + authenticate (re-run on every reconnect)
    async def request(self, endpoint, payload=None, query=None): ...  # one RPC exchange
    async def heartbeat(self):      ...  # keepalive / token renewal
    async def run(self, ctx):       ...  # optional: event read loop -> ctx.enqueue(...)
    async def handle_control(self, action, data, ctx): ...  # resync etc.

def get_connections():
    return [MyServiceConnection()]
```

Call it from anywhere inside Frappe:

```python
from frappe_persistent_ws import client

result = client.call("my-service", "user/find", payload={"name": "x"}, timeout=30)
client.publish_control("my-service", "resync")
```

Run the daemon (one per site; a file lock makes extra invocations exit):

```bash
bench --site mysite persistent-ws
```

In production, run it under supervisor exactly like `bench schedule`:

```ini
[program:frappe-bench-persistent-ws]
command=/usr/local/bin/bench persistent-ws --site mysite
autostart=true
autorestart=true
```

## Design notes

- The bus is the **queue redis** (raw, unprefixed keys, JSON only) — the same
  instance Frappe realtime uses as its Python↔Node bridge.
- The daemon is deliberately thin: it never writes to the database. Events and
  state changes are handed to normal workers via `frappe.enqueue`.
- Reconnects use jittered, capped exponential backoff — a provider-side flap never
  turns into a handshake storm.
- Crash-fast: any unrecoverable error exits the process; supervision restarts it
  (the same recovery model as Frappe's realtime node server).

## Installation

You can install this app using the [bench](https://github.com/frappe/bench) CLI:

```bash
cd $PATH_TO_YOUR_BENCH
bench get-app $URL_OF_THIS_REPO --branch main
bench install-app frappe_persistent_ws
```

## Contributing

This app uses `pre-commit` for code formatting and linting. Please [install
pre-commit](https://pre-commit.com/#installation) and enable it for this repository:

```bash
cd apps/frappe_persistent_ws
pre-commit install
```

Pre-commit is configured to use the following tools for checking and formatting
your code:

- ruff
- semgrep (frappe rules)
- eslint
- prettier

## License

agpl-3.0
