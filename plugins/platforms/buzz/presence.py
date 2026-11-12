"""Signed Buzz presence + runtime-record events for the gateway adapter.

Kind:20001 (ephemeral, WebSocket-only — the HTTP bridge rejects ephemeral kinds) carries a bare
status string; the relay stores it in Redis with a 180 s TTL (three 60 s heartbeat windows), so a
single missed heartbeat cannot flap presence and a crash expires within three minutes. Kind:10100
is a replaceable agent-authored runtime record whose explicit ``status`` survives the owner-policy
overlay in owned-agent discovery (New Message / member-add / search); its other fields are
untrusted there, so only truthful ones are written. Kind:20002 is the ephemeral typing indicator,
channel-scoped by an ``h`` tag.

All events are signed with the same dependency-free BIP-340 signer used for NIP-42 AUTH
(``nostr_auth.py``) — no new dependencies.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Optional


def _nostr_auth():
    """The sibling BIP-340 signer (package-relative when available, else loaded by path — the test
    loader imports adapter files as bare modules without package context)."""
    try:
        from . import nostr_auth
        return nostr_auth
    except ImportError:
        import importlib.util
        from pathlib import Path

        path = Path(__file__).with_name("nostr_auth.py")
        spec = importlib.util.spec_from_file_location(f"plugin_adapter_buzz_nostr_auth", path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


def build_signed_event(
    private_key: str, kind: int, content: str, tags: Optional[list] = None,
    created_at: Optional[int] = None,
) -> dict[str, Any]:
    """Serialize + sign one Nostr event (same canonical form as the NIP-42 AUTH events)."""
    _nostr = _nostr_auth()

    pubkey = _nostr.public_key_hex(private_key)
    timestamp = int(time.time()) if created_at is None else int(created_at)
    tags = list(tags or [])
    serialized = json.dumps(
        [0, pubkey, timestamp, kind, tags, content], separators=(",", ":"), ensure_ascii=False
    ).encode()
    event_id = hashlib.sha256(serialized).digest()
    return {
        "id": event_id.hex(),
        "pubkey": pubkey,
        "created_at": timestamp,
        "kind": kind,
        "tags": tags,
        "content": content,
        "sig": _nostr.schnorr_sign(event_id, private_key).hex(),
    }


def presence_event(private_key: str, status: str) -> dict[str, Any]:
    """Kind:20001 — bare status string (``online``/``away``/``offline``), no tags (desktop format)."""
    return build_signed_event(private_key, 20001, status)


def runtime_record_event(
    private_key: str, *, name: str = "", owner_pubkey: str = "", status: str = "online",
    created_at: Optional[int] = None,
) -> dict[str, Any]:
    """Kind:10100 — replaceable runtime record; discovery trusts only the explicit ``status``.

    *created_at* exists for writer discipline: NIP-16 replacement orders by
    ``created_at DESC, id ASC``, so a same-second republish can lose the tie-break to a random id."""
    content: dict[str, Any] = {}
    if name:
        content["name"] = name
    if owner_pubkey:
        content["owner_pubkey"] = owner_pubkey
    content["status"] = status
    return build_signed_event(
        private_key, 10100, json.dumps(content, separators=(",", ":")), created_at=created_at)


def typing_event(private_key: str, channel_id: str) -> dict[str, Any]:
    """Kind:20002 — ephemeral typing indicator scoped to one channel via the ``h`` tag."""
    return build_signed_event(private_key, 20002, "", tags=[["h", str(channel_id)]])
