"""Durable per-channel receive cursors for the Buzz adapter (#90464): a restart resumes from the saved
cursor instead of reseeding from history and marking downtime arrivals as seen."""
import asyncio
import json
import logging
from collections import OrderedDict
from pathlib import Path

logger = logging.getLogger(__name__)

_SEEN_CAP = 500  # per-channel de-dupe set bound (events)
_JOINED_THREADS_STATE_CAP = 256  # thread roots the agent posted into (send-side follow)
_CURSOR_STATE_SUBDIR = "buzz"  # per-channel cursors survive a restart under HERMES_HOME
_CURSOR_STATE_FILENAME = "channel-cursors.json"


class BuzzCursorMixin:
    _joined_threads: OrderedDict[str, None]  # owned by BuzzAdapter.__init__

    @staticmethod
    def _cursor_path() -> Path:
        from hermes_constants import get_hermes_home
        return get_hermes_home() / _CURSOR_STATE_SUBDIR / _CURSOR_STATE_FILENAME

    def _load_cursors(self) -> None:
        """Read persisted cursors; another identity/relay's file is ignored (ids collide), failures seed from history."""
        self._restored_cursors = {}
        try:
            if not (path := self._cursor_path()).exists():
                return
            data = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            logger.debug("Buzz: could not read channel cursors", exc_info=True)
            return
        if not isinstance(data, dict) or data.get("identity") != self._self_pubkey or data.get("relay") != self.relay_url:
            return
        if not isinstance(channels := data.get("channels"), dict):
            return
        for channel_id, entry in channels.items():
            if not isinstance(entry, dict):
                continue
            try:
                last_ts = int(entry.get("last_ts") or 0)
            except (TypeError, ValueError):
                continue
            raw_seen = entry.get("seen")
            seen = [str(event_id) for event_id in raw_seen][-_SEEN_CAP:] if isinstance(raw_seen, list) else []
            self._restored_cursors[str(channel_id)] = {"chat_type": str(entry.get("chat_type") or ""), "last_ts": last_ts, "seen": seen}
        raw_threads = data.get("joined_threads")
        if isinstance(raw_threads, list):
            self._joined_threads.clear()
            for thread_root in raw_threads[-_JOINED_THREADS_STATE_CAP:]:
                if root := str(thread_root or ""):
                    self._joined_threads[root] = None

    def _cursor_payload(self) -> dict:
        """Snapshot of every watched channel's cursor (taken on the loop: ``_channel_state`` is loop-owned)."""
        channels = {
            channel_id: {
                "chat_type": state.get("chat_type") or "group", "last_ts": int(state.get("last_ts") or 0),
                "seen": list(state.get("seen") or ()),
            }
            for channel_id, state in self._channel_state.items()
        }
        return {"identity": self._self_pubkey, "relay": self.relay_url, "channels": channels,
                "joined_threads": list(self._joined_threads)[-_JOINED_THREADS_STATE_CAP:]}

    def _save_cursors(self) -> None:
        """Persist every watched channel's cursor.  Never raises."""
        self._write_cursors(self._cursor_path(), self._cursor_payload())

    @staticmethod
    def _write_cursors(path: Path, payload: dict) -> None:
        try:
            from utils import atomic_json_write
            atomic_json_write(path, payload, indent=None)
        except Exception:
            logger.debug("Buzz: could not persist channel cursors", exc_info=True)

    @staticmethod
    def _cursor_mark(state: dict) -> tuple:
        """Cheap change detector for one channel's cursor."""
        seen = state.get("seen") or ()
        return int(state.get("last_ts") or 0), len(seen), (next(reversed(seen), None) if seen else None)

    def _cursor_writer(self):
        """A write of the cursor as it stands now. A deferred write runs after the reader marked later
        events seen before their dispatch committed, so it must never re-read the live state; and a
        write older than one already landed is skipped, so the file never moves back."""
        payload = self._cursor_payload()
        self._cursor_snapshots = generation = self._cursor_snapshots + 1

        async def persist() -> None:
            # The write fsyncs + renames, and on the WebSocket transport this runs once per inbound event.
            async with self._cursor_write_lock:
                if generation > self._cursor_written:
                    self._cursor_written = generation
                    await asyncio.to_thread(self._write_cursors, self._cursor_path(), payload)
        return persist
