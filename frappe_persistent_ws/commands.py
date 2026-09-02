"""Bench command: run the persistent-WS daemon for one site.

    bench --site <site> persistent-ws

Runs forever under supervision (a supervisor ``[program:]`` in production —
mirroring how ``bench schedule`` and the realtime node server are run — or a
plain terminal in dev).  A bench-local file lock makes it a singleton per site:
a second invocation exits immediately, the same pattern ``bench schedule`` uses.
"""

from __future__ import annotations

import asyncio
import signal

import click


@click.command("persistent-ws")
@click.option("--site", help="Site to run the daemon for (defaults to the bench default site)")
def persistent_ws(site: str | None = None):
    """Run the persistent WebSocket broker daemon (singleton per site)."""
    import frappe
    from filelock import FileLock, Timeout
    from frappe.utils import get_sites

    if not site:
        site = frappe.get_conf().get("default_site") or (get_sites() or [None])[0]
    if not site:
        raise click.UsageError("No site given and no default site configured — pass --site.")

    frappe.init(site=site)
    lock = FileLock(frappe.get_site_path("locks", "persistent_ws_daemon.lock"))
    try:
        lock.acquire(timeout=0.1)
    except Timeout:
        click.echo(f"persistent-ws is already running for {site} — exiting.")
        return

    try:
        frappe.connect()
        from frappe_persistent_ws.daemon import PersistentWSDaemon

        daemon = PersistentWSDaemon(site)
        # Echo the daemon's logger to stdout: frappe.logger writes files only, and a
        # silent terminal while the supervisor loop retries in the background is
        # exactly how connection failures go unnoticed.  Supervisor captures stdout
        # too, so production gets the same visibility.
        import logging
        import sys

        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        daemon.logger.addHandler(console)
        daemon.logger.setLevel(logging.INFO)

        loop = asyncio.new_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, daemon.request_stop)
        try:
            loop.run_until_complete(daemon.run())
        finally:
            loop.close()
    finally:
        lock.release()
        frappe.destroy()


commands = [persistent_ws]
