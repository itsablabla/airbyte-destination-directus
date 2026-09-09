"""Minimal Airbyte destination for Directus (protocol-compatible, no CDK)."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

LOGGER = logging.getLogger("airbyte.destination_directus")

DEFAULT_COLLECTION_MAP = {
    "accounts": "beeper_accounts",
    "bridges": "beeper_bridges",
    "chats": "beeper_chats",
    "chats_search": "beeper_chats",
    "messages_search": "beeper_messages",
    "contacts": "beeper_contacts",
}

DEFAULT_PK_MAP = {
    "accounts": "account_id",
    "bridges": "bridge_id",
    "chats": "chat_id",
    "chats_search": "chat_id",
    "messages_search": "message_id",
    "contacts": "contact_key",
}


def _now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _emit(msg: Mapping[str, Any]) -> None:
    sys.stdout.write(json.dumps(msg, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def _log(level: str, message: str) -> None:
    _emit({"type": "LOG", "log": {"level": level, "message": message}})


class DirectusClient:
    def __init__(self, url: str, token: str):
        self.base = url.rstrip("/")
        self.token = token

    def _request(self, method: str, path: str, payload: Any = None, params: Optional[dict] = None) -> Any:
        qs = ("?" + urllib.parse.urlencode(params, doseq=True)) if params else ""
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.base + path + qs,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "airbyte-destination-directus/0.1.0",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
                return json.loads(raw.decode("utf-8")) if raw else None
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Directus {method} {path} failed: HTTP {e.code}: {body[:800]}") from e

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> "DirectusClient":
        url = str(config["url"]).rstrip("/")
        token = (config.get("token") or "").strip()
        if not token:
            email = config.get("email")
            password = config.get("password")
            if not email or not password:
                raise ValueError("Provide either token, or email+password")
            req = urllib.request.Request(
                url + "/auth/login",
                data=json.dumps({"email": email, "password": password}).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json", "User-Agent": "airbyte-destination-directus/0.1.0"},
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            token = data["data"]["access_token"]
        return cls(url, token)

    def ping(self) -> None:
        self._request("GET", "/users/me")

    def clear_collection(self, collection: str) -> None:
        self._request("DELETE", f"/items/{collection}", params={"limit": -1})

    def find_id(self, collection: str, key: str, value: Any) -> Optional[Any]:
        filt = json.dumps({key: {"_eq": value}})
        data = self._request(
            "GET",
            f"/items/{collection}",
            params={"filter": filt, "fields": "id", "limit": 1},
        )
        rows = (data or {}).get("data") or []
        if not rows:
            return None
        return rows[0].get("id")

    def upsert_many(self, collection: str, key: str, rows: List[dict]) -> Tuple[int, int]:
        created = updated = 0
        for row in rows:
            pk = row.get(key)
            if pk is None:
                self._request("POST", f"/items/{collection}", payload=row)
                created += 1
                continue
            existing = self.find_id(collection, key, pk)
            if existing is None:
                self._request("POST", f"/items/{collection}", payload=row)
                created += 1
            else:
                self._request("PATCH", f"/items/{collection}/{existing}", payload=row)
                updated += 1
        return created, updated


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def transform_record(stream: str, data: Mapping[str, Any], pk_field: str) -> dict:
    row = dict(data)
    row["synced_at"] = _now_iso()
    if "raw" not in row:
        row["raw"] = data

    if stream == "accounts":
        row.setdefault("account_id", data.get("accountID") or data.get("id") or data.get("account_id"))
        row.setdefault("login_id", data.get("loginID") or data.get("login_id"))
        if isinstance(data.get("user"), dict):
            row["user"] = data["user"]
        if isinstance(data.get("bridge"), dict):
            row["bridge"] = data["bridge"]
        if "capabilities" in data:
            row["capabilities"] = data["capabilities"]
        row.setdefault("network", data.get("network"))
        row.setdefault("status", data.get("status"))
        row.setdefault("status_text", data.get("statusText") or data.get("status_text"))
    elif stream == "bridges":
        row.setdefault("bridge_id", data.get("bridgeID") or data.get("id") or data.get("bridge_id"))
        row.setdefault("display_name", data.get("displayName") or data.get("display_name"))
        row.setdefault("active_account_count", data.get("activeAccountCount") or data.get("active_account_count"))
        if "supportsMultipleAccounts" in data:
            row.setdefault("supports_multiple_accounts", data["supportsMultipleAccounts"])
        else:
            row.setdefault("supports_multiple_accounts", data.get("supports_multiple_accounts"))
        row.setdefault("status_text", data.get("statusText") or data.get("status_text"))
    elif stream in ("chats", "chats_search"):
        row.setdefault("chat_id", data.get("chatID") or data.get("id") or data.get("chat_id"))
        row.setdefault("local_chat_id", data.get("localChatID") or data.get("local_chat_id"))
        row.setdefault("account_id", data.get("accountID") or data.get("account_id"))
        row.setdefault("img_url", data.get("imgURL") or data.get("imgUrl") or data.get("img_url"))
        if "isReadOnly" in data:
            row.setdefault("is_read_only", data["isReadOnly"])
        row.setdefault("last_activity", data.get("lastActivity") or data.get("last_activity"))
        if "unreadCount" in data:
            row.setdefault("unread_count", data["unreadCount"])
        if "unreadMentionsCount" in data:
            row.setdefault("unread_mentions_count", data["unreadMentionsCount"])
        for src, dst in [
            ("isArchived", "is_archived"),
            ("isMuted", "is_muted"),
            ("isPinned", "is_pinned"),
            ("isLowPriority", "is_low_priority"),
        ]:
            if src in data:
                row.setdefault(dst, data[src])
    elif stream == "messages_search":
        row.setdefault("message_id", data.get("messageID") or data.get("id") or data.get("message_id"))
        row.setdefault("chat_id", data.get("chatID") or data.get("chat_id"))
        row.setdefault("account_id", data.get("accountID") or data.get("account_id"))
        row.setdefault("sender_id", data.get("senderID") or data.get("sender_id"))
        row.setdefault("sender_name", data.get("senderName") or data.get("sender_name"))
        row.setdefault("sort_key", data.get("sortKey") or data.get("sort_key"))
        for src, dst in [
            ("isSender", "is_sender"),
            ("isDeleted", "is_deleted"),
            ("isUnread", "is_unread"),
        ]:
            if src in data:
                row.setdefault(dst, data[src])
    elif stream == "contacts":
        contact_id = data.get("contactID") or data.get("id") or data.get("contact_id")
        account_id = data.get("accountID") or data.get("account_id")
        row.setdefault("contact_id", contact_id)
        row.setdefault("account_id", account_id)
        row.setdefault("contact_key", data.get("contact_key") or f"{account_id}:{contact_id}")
        row.setdefault("phone_number", data.get("phoneNumber") or data.get("phone_number"))
        row.setdefault("full_name", data.get("fullName") or data.get("full_name"))
        row.setdefault("img_url", data.get("imgURL") or data.get("imgUrl") or data.get("img_url"))
        if "cannotMessage" in data:
            row.setdefault("cannot_message", data["cannotMessage"])
        if "isSelf" in data:
            row.setdefault("is_self", data["isSelf"])

    row.pop("id", None)
    if pk_field not in row or row[pk_field] in (None, ""):
        raise ValueError(f"Missing primary key '{pk_field}' for stream {stream}")
    return row


class DestinationDirectus:
    def spec(self) -> dict:
        with open("/airbyte/integration_code/destination_directus/spec.json", "r", encoding="utf-8") as f:
            return json.load(f)

    def check(self, config: Mapping[str, Any]) -> dict:
        try:
            client = DirectusClient.from_config(config)
            client.ping()
            return {"status": "SUCCEEDED"}
        except Exception as e:
            return {"status": "FAILED", "message": str(e)}

    def write(self, config: Mapping[str, Any], catalog: Mapping[str, Any], messages: Iterable[dict]) -> None:
        client = DirectusClient.from_config(config)
        collection_map = {**DEFAULT_COLLECTION_MAP, **(config.get("collection_map") or {})}
        pk_map = {**DEFAULT_PK_MAP, **(config.get("primary_key_map") or {})}
        batch_size = int(config.get("batch_size") or 50)

        stream_meta = {}
        for s in catalog.get("streams", []):
            name = s.get("stream", {}).get("name") or s.get("name")
            mode = s.get("destination_sync_mode") or "append"
            stream_meta[name] = mode

        cleared = set()
        for stream, mode in stream_meta.items():
            if mode == "overwrite":
                collection = collection_map.get(stream, stream)
                if collection not in cleared:
                    _log("INFO", f"Overwrite mode: clearing Directus collection {collection}")
                    try:
                        client.clear_collection(collection)
                    except Exception as e:
                        _log("WARN", f"Clear {collection} failed (continuing): {e}")
                    cleared.add(collection)

        buffers: Dict[str, List[dict]] = defaultdict(list)
        written = defaultdict(int)

        def flush(stream: str) -> None:
            rows = buffers[stream]
            if not rows:
                return
            collection = collection_map.get(stream, stream)
            pk = pk_map.get(stream, "id")
            created, updated = client.upsert_many(collection, pk, rows)
            written[stream] += created + updated
            _log("INFO", f"Flushed {len(rows)} rows to {collection} (created={created}, updated={updated})")
            buffers[stream] = []

        for msg in messages:
            mtype = msg.get("type")
            if mtype == "RECORD":
                rec = msg["record"]
                stream = rec["stream"]
                if stream not in stream_meta:
                    continue
                pk = pk_map.get(stream, "id")
                try:
                    row = transform_record(stream, rec.get("data") or {}, pk)
                except Exception as e:
                    _log("ERROR", f"Skip record in {stream}: {e}")
                    continue
                buffers[stream].append(row)
                if len(buffers[stream]) >= batch_size:
                    flush(stream)
            elif mtype == "STATE":
                for stream in list(buffers.keys()):
                    flush(stream)
                _emit(msg)
            elif mtype == "LOG":
                _emit(msg)

        for stream in list(buffers.keys()):
            flush(stream)
        _log("INFO", f"Write complete: {dict(written)}")


def _iter_stdin_messages() -> Iterable[dict]:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            _log("WARN", f"Skipping non-JSON stdin line: {line[:120]}")


def run(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(prog="destination-directus")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("spec")
    p_check = sub.add_parser("check")
    p_check.add_argument("--config", required=True)
    p_write = sub.add_parser("write")
    p_write.add_argument("--config", required=True)
    p_write.add_argument("--catalog", required=True)

    args = parser.parse_args(argv)
    dest = DestinationDirectus()

    if args.command == "spec":
        # allow running outside docker too
        try:
            spec = dest.spec()
        except FileNotFoundError:
            local = Path(__file__).with_name("spec.json")
            with open(local, "r", encoding="utf-8") as f:
                spec = json.load(f)
        _emit({"type": "SPEC", "spec": spec})
        return

    config = _load_json(args.config)
    if args.command == "check":
        status = dest.check(config)
        _emit({"type": "CONNECTION_STATUS", "connectionStatus": status})
        return

    if args.command == "write":
        catalog = _load_json(args.catalog)
        dest.write(config, catalog, _iter_stdin_messages())
        return


# late import for Path used in run()
from pathlib import Path  # noqa: E402

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
