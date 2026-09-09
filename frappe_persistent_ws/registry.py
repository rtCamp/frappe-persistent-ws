"""Resolve the site's persistent connections from installed apps.

Consumer apps declare factories in their ``hooks.py``::

    persistent_ws_connections = [
        "my_app.integrations.bridge.get_connections",
    ]

Each factory is called with no arguments and returns a
:class:`~frappe_persistent_ws.connection.BasePersistentConnection` instance or a
list of them.  Factories run at daemon startup with a full site context, so they
may read site config/doctypes to decide which connections to expose (e.g. skip a
connection whose feature flag is off).
"""

from __future__ import annotations

import frappe

from frappe_persistent_ws.connection import BasePersistentConnection

HOOK = "persistent_ws_connections"


def get_connections() -> list[BasePersistentConnection]:
    """Instantiate every hooked connection, validating names are unique + non-empty."""
    connections: list[BasePersistentConnection] = []
    for path in frappe.get_hooks(HOOK) or []:
        factory = frappe.get_attr(path)
        result = factory()
        items = list(result) if isinstance(result, (list, tuple)) else [result]
        for item in items:
            if item is None:
                continue
            if not isinstance(item, BasePersistentConnection):
                frappe.throw(f"{path} returned {type(item).__name__}, not a BasePersistentConnection")
            connections.append(item)

    seen: set[str] = set()
    for conn in connections:
        if not conn.name:
            frappe.throw(f"Persistent connection {type(conn).__name__} has no name")
        if conn.name in seen:
            frappe.throw(f"Duplicate persistent connection name: {conn.name}")
        seen.add(conn.name)
    return connections
