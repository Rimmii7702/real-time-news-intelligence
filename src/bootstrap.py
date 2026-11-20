"""Idempotently create Kafka topics and mapped Elasticsearch indices."""
from __future__ import annotations
import argparse

from kafka.admin import KafkaAdminClient, NewTopic
import requests

from init_indices import INDEX_MAPPINGS

TOPICS = ("news-raw", "entity-counts", "article-insights")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default="kafka:29092")
    p.add_argument("--es-url", default="http://elasticsearch:9200")
    args = p.parse_args()

    client = KafkaAdminClient(bootstrap_servers=args.bootstrap, client_id="news-intelligence-bootstrap")
    try:
        existing = set(client.list_topics())
        missing = [name for name in TOPICS if name not in existing]
        if missing:
            client.create_topics([NewTopic(name=name, num_partitions=1, replication_factor=1)
                                  for name in missing])
        print("Kafka topics ready:", ", ".join(TOPICS))
    finally:
        client.close()

    root = args.es_url.rstrip("/")
    for name, props in INDEX_MAPPINGS.items():
        url = root + "/" + name
        res = requests.put(url, json={"mappings": {"properties": props}}, timeout=30)
        if res.status_code == 400 and "resource_already_exists_exception" in res.text:
            print("Elasticsearch index exists:", name)
        else:
            res.raise_for_status()
            print("Elasticsearch index created:", name)


if __name__ == "__main__":
    main()
