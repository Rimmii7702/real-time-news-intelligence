"""Poll current public RSS news feeds, deduplicate articles, publish to Kafka."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import time

import feedparser
from kafka import KafkaProducer
import requests

from common import article_key, plain_text

DEFAULT_FEEDS = [
    "https://feeds.bbci.co.uk/news/rss.xml",
    "https://feeds.bbci.co.uk/news/world/rss.xml",
]


def args_parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", required=True, help="Kafka server, e.g. localhost:9092")
    p.add_argument("--topic", default="news-raw")
    p.add_argument("--feed", action="append", dest="feeds", help="RSS URL; repeat option")
    p.add_argument("--state-db", type=Path, required=True, help="SQLite dedup database path")
    p.add_argument("--start-marker", type=Path, required=True, help="Run start metadata JSON path")
    p.add_argument("--poll-seconds", type=int, default=60)
    p.add_argument("--limit-per-feed", type=int, default=100)
    p.add_argument("--max-polls", type=int, default=0, help="0 runs forever; 1 runs once for testing")
    return p


def fetch_items(session: requests.Session, url: str, limit: int) -> list[dict]:
    response = session.get(url, timeout=25, headers={"User-Agent": "NewsIntelligencePortfolio/1.0"})
    response.raise_for_status()
    feed = feedparser.parse(response.content)
    if feed.bozo and not feed.entries:
        raise ValueError(f"RSS parsing failed for {url}: {feed.bozo_exception}")
    items = []
    for entry in feed.entries[:limit]:
        title = plain_text(entry.get("title", ""))
        summary = plain_text(entry.get("summary", entry.get("description", "")))
        combined = f"{title}. {summary}".strip()
        if not combined or combined == ".":
            continue
        raw = dict(entry)
        items.append({
            "article_id": article_key(raw),
            "text": combined[:12000],
            "title": title,
            "url": entry.get("link", ""),
            "published_at": entry.get("published", ""),
            "feed_url": url,
            "observed_at": datetime.now(timezone.utc).isoformat(),
        })
    return items


def init_db(connection: sqlite3.Connection) -> None:
    connection.execute("CREATE TABLE IF NOT EXISTS sent (article_id TEXT PRIMARY KEY, observed_at TEXT NOT NULL)")
    connection.commit()


def publish_items(items: list[dict], connection: sqlite3.Connection, producer, topic: str,
                  start_marker: Path | None = None, feeds: list[str] | None = None) -> int:
    published = 0
    for item in items:
        exists = connection.execute("SELECT 1 FROM sent WHERE article_id=?", (item["article_id"],)).fetchone()
        if exists:
            continue
        # Kafka ack first, commit dedup state second. Retries can replay at most
        # some messages after crash; the sink is upsert-by-entity, not exactly-once.
        producer.send(topic, key=item["article_id"].encode("utf-8"), value=item).get(timeout=30)
        # Start the observation only after Kafka acknowledges real input.
        # Failed connection/setup attempts must not start the 60-minute clock.
        if start_marker is not None and not start_marker.exists():
            start_marker.parent.mkdir(parents=True, exist_ok=True)
            start_marker.write_text(json.dumps({
                "started_at_utc": datetime.now(timezone.utc).isoformat(),
                "feeds": feeds or DEFAULT_FEEDS,
                "first_article_id": item["article_id"],
            }, indent=2), encoding="utf-8")
        connection.execute(
            "INSERT OR IGNORE INTO sent(article_id, observed_at) VALUES(?, ?)",
            (item["article_id"], item["observed_at"]),
        )
        connection.commit()
        published += 1
    producer.flush(timeout=30)
    return published


def main():
    args = args_parser().parse_args()
    if args.poll_seconds < 1 or args.limit_per_feed < 1 or args.max_polls < 0:
        raise SystemExit("Invalid poll/limit arguments")
    args.state_db.parent.mkdir(parents=True, exist_ok=True)
    args.start_marker.parent.mkdir(parents=True, exist_ok=True)
    if args.start_marker.exists():
        print(f"Reusing start marker {args.start_marker}; delete it ONLY for a new run.")

    connection = sqlite3.connect(args.state_db)
    init_db(connection)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda item: json.dumps(item, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        max_block_ms=30000,
    )
    session = requests.Session()
    feeds = args.feeds or DEFAULT_FEEDS
    polls = 0
    try:
        while True:
            for url in feeds:
                try:
                    articles = fetch_items(session, url, args.limit_per_feed)
                    sent = publish_items(articles, connection, producer, args.topic,
                                         args.start_marker, feeds)
                    print(f"[{datetime.now(timezone.utc).isoformat()}] {url}: fetched {len(articles)}; new {sent}", flush=True)
                except Exception as exc:
                    # Continue polling other feeds; report failures rather than
                    # pretending fresh data is arriving.
                    print(f"FEED ERROR: {url}: {type(exc).__name__}: {exc}", flush=True)
            polls += 1
            if args.max_polls and polls >= args.max_polls:
                return
            time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        print("Producer stopped")
    finally:
        producer.flush(timeout=30)
        producer.close()
        session.close()
        connection.close()


if __name__ == "__main__":
    main()
