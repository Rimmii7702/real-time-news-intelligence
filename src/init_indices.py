"""Set up keyword/numeric/date Elasticsearch fields before Logstash starts."""
from __future__ import annotations
import argparse
import requests

INDEX_MAPPINGS = {
    "entity-counts-current": {
        "entity_id": {"type": "keyword"},
        "entity": {"type": "keyword"},
        "entity_type": {"type": "keyword"},
        "count": {"type": "long"},
        "batch_id": {"type": "long"},
        "updated_at": {"type": "date"},
    },
    "news-article-insights": {
        "article_id": {"type": "keyword"},
        "title": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
        "text": {"type": "text"},
        "category": {"type": "keyword"},
        "classification_model": {"type": "keyword"},
        "url": {"type": "keyword", "ignore_above": 2048},
        "feed_url": {"type": "keyword"},
        "published_at": {"type": "keyword"},
        "observed_at": {"type": "date"},
        "processed_at": {"type": "date"},
    },
}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--es-url", default="http://elasticsearch:9200")
    p.add_argument("--reset", action="store_true", help="DELETE AND RECREATE BOTH INDICES")
    a = p.parse_args()
    root = a.es_url.rstrip("/")
    for name, properties in INDEX_MAPPINGS.items():
        url = root + "/" + name
        if a.reset:
            response = requests.delete(url, timeout=20)
            if response.status_code not in (200, 404):
                response.raise_for_status()
        response = requests.put(url, json={"mappings": {"properties": properties}}, timeout=30)
        if response.status_code == 400 and "resource_already_exists_exception" in response.text:
            print(f"Existing index: {name}")
        else:
            response.raise_for_status()
            print(f"Created index: {name}")


if __name__ == "__main__":
    main()
