#!/usr/bin/env python3
"""export-keyring.py — run on the MAIN machine (over ssh).

Dumps the Chrome/Chromium "Safe Storage" os_crypt secrets from gnome-keyring
to stdout as JSON. Chrome profile cookies are v11-encrypted with these keys —
without them a synced profile is a signed-out shell.

Usage on standby:  ssh main 'DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$UID/bus \
                     python3 -' < export-keyring.py > ~/.flowkit-keyring.json
"""
import json
import sys

import dbus

WANTED = {
    "chrome": "Chrome Safe Storage",
    "chromium": "Chromium Safe Storage",
}
SCHEMA = "chrome_libsecret_os_crypt_password_v2"


def main() -> int:
    bus = dbus.SessionBus()
    svc = bus.get_object("org.freedesktop.secrets", "/org/freedesktop/secrets")
    service = dbus.Interface(svc, "org.freedesktop.Secret.Service")
    _, session = service.OpenSession("plain", "")

    coll = bus.get_object(
        "org.freedesktop.secrets", "/org/freedesktop/secrets/collection/login")
    props = dbus.Interface(coll, "org.freedesktop.DBus.Properties")
    items = props.Get("org.freedesktop.Secret.Collection", "Items")

    out = {}
    for path in items:
        item = bus.get_object("org.freedesktop.secrets", str(path))
        iprops = dbus.Interface(item, "org.freedesktop.DBus.Properties")
        attrs = dict(iprops.Get("org.freedesktop.Secret.Item", "Attributes"))
        app = str(attrs.get("application", ""))
        if app not in WANTED or str(attrs.get("xdg:schema", "")) != SCHEMA:
            continue
        itf = dbus.Interface(item, "org.freedesktop.Secret.Item")
        _, _, secret, _ = itf.GetSecret(dbus.ObjectPath(session))
        out[app] = {
            "label": str(iprops.Get("org.freedesktop.Secret.Item", "Label")),
            "secret": bytes(secret).decode("utf-8", "replace"),
        }

    if "chrome" not in out:
        print("no Chrome Safe Storage secret found", file=sys.stderr)
        return 1
    print(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
