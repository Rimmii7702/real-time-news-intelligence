"""Audit indexed articles, NER count totals, categories, and observed->processed lag.

Observed->processed is an ingestion-to-Spark proxy, not network-to-Kibana latency.
Run after Kafka, Spark and Logstash have had time to catch up.
"""
from __future__ import annotations
import argparse
from datetime import datetime
import json
from pathlib import Path

import requests
from report_stream_metrics import percentile


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def get_json(session: requests.Session, url: str, **kwargs) -> dict:
    res = session.get(url, timeout=30, **kwargs)
    res.raise_for_status()
    return res.json()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--es-url", default="http://localhost:9200")
    p.add_argument("--max-docs", type=int, default=5000)
    p.add_argument("--output", default="metrics/index_audit.json")
    a = p.parse_args()
    root = a.es_url.rstrip("/")
    session = requests.Session()
    session.headers.update({"User-Agent": "NewsIntelligenceAudit/1.0"})

    entity = get_json(session, root + "/entity-counts-current/_search", params={"size": "0"}, json={
        "aggs": {"mentions": {"sum": {"field": "count"}}}
    })
    categories = get_json(session, root + "/news-article-insights/_search", params={"size": "0"}, json={
        "aggs": {"categories": {"terms": {"field": "category", "size": 10}}}
    })

    lag_ms = []
    cursor = None
    sampled = 0
    while sampled < a.max_docs:
        body = {"size": min(500, a.max_docs - sampled), "sort": [{"article_id": "asc"}],
                "_source": ["article_id", "observed_at", "processed_at"]}
        if cursor:
            body["search_after"] = cursor
        page = get_json(session, root + "/news-article-insights/_search", json=body)
        hits = page["hits"]["hits"]
        if not hits:
            break
        for hit in hits:
            sampled += 1
            record = hit["_source"]
            observed = parse_iso(record.get("observed_at"))
            processed = parse_iso(record.get("processed_at"))
            if observed and processed:
                delta = (processed - observed).total_seconds() * 1000
                if delta >= 0:
                    lag_ms.append(delta)
        cursor = hits[-1]["sort"]

    entity_count = get_json(session, root + "/entity-counts-current/_count")["count"]
    article_count = get_json(session, root + "/news-article-insights/_count")["count"]
    report = {
        "indexed_articles": article_count,
        "distinct_entity_type_pairs": entity_count,
        "cumulative_entity_mentions": int(entity["aggregations"]["mentions"]["value"] or 0),
        "category_counts": {item["key"]: item["doc_count"] for item in categories["aggregations"]["categories"]["buckets"]},
        "latency_sampled_articles": sampled,
        "lag_samples_with_valid_timestamps": len(lag_ms),
        "observed_to_spark_processed_p50_ms": round(percentile(lag_ms, 50), 2) if lag_ms else None,
        "observed_to_spark_processed_p95_ms": round(percentile(lag_ms, 95), 2) if lag_ms else None,
        "note": "Lag excludes downstream Kafka->Logstash->Elasticsearch indexing; accuracy requires labeled data.",
    }
    output = Path(a.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
