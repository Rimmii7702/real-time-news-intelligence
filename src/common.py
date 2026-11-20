"""Pure helpers shared by source producer and Spark stream (no runtime services)."""
from __future__ import annotations
from hashlib import sha256
from html import unescape
from html.parser import HTMLParser
import re


class _PlainText(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)


def plain_text(raw: str | None) -> str:
    parser = _PlainText()
    parser.feed(raw or "")
    return re.sub(r"\s+", " ", unescape(" ".join(parser.parts))).strip()


def normalize_entity(entity: str) -> str:
    return re.sub(r"\s+", " ", entity).strip().casefold()


def entity_key(entity: str, label: str) -> str:
    """Stable document ID for latest count of a (entity, NER class) pair."""
    return sha256((label + "\x00" + normalize_entity(entity)).encode("utf-8")).hexdigest()


def article_key(item: dict) -> str:
    """Prefer a permalink or item GUID, so a repeated RSS poll is deduplicated."""
    identity = item.get("id") or item.get("guid") or item.get("link")
    if not identity:
        identity = item.get("title", "") + "|" + item.get("published", "")
    return sha256(str(identity).encode("utf-8")).hexdigest()
