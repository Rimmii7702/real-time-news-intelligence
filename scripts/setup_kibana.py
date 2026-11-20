"""Create Kibana data views and the cumulative entity top-10 bar visualization."""
from __future__ import annotations
import argparse
import json
import requests


def setup_view(session: requests.Session, base: str, view_id: str, index: str) -> None:
    response = session.post(base + "/api/data_views/data_view", json={
        "data_view": {"id": view_id, "title": index, "name": index},
        "override": True,
    }, timeout=60)
    response.raise_for_status()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--kibana-url", default="http://kibana:5601")
    a = p.parse_args()
    base = a.kibana_url.rstrip("/")
    session = requests.Session()
    session.headers["kbn-xsrf"] = "news-intelligence"
    setup_view(session, base, "news-entity-view", "entity-counts-current")
    setup_view(session, base, "news-article-view", "news-article-insights")

    visualization = {
        "title": "News Intelligence | Top Named Entities",
        "type": "horizontal_bar",
        "params": {"addLegend": False, "addTooltip": True, "showValuesOnChart": True},
        "aggs": [
            {"id": "1", "enabled": True, "type": "sum", "schema": "metric", "params": {
                "field": "count", "customLabel": "Cumulative mentions"
            }},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment", "params": {
                "field": "entity", "size": 10, "order": "desc", "orderBy": "1",
                "otherBucket": False, "missingBucket": False
            }},
        ],
    }
    response = session.post(base + "/api/saved_objects/visualization/news-entity-top10",
        params={"overwrite": "true"}, json={
            "attributes": {
                "title": visualization["title"],
                "visState": json.dumps(visualization),
                "uiStateJSON": "{}",
                "description": "Top entities ranked by sum of current (entity, entity_type) mentions.",
                "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "query": {"query": "", "language": "kuery"}, "filter": [],
                })},
            },
            "references": [{
                "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                "type": "index-pattern", "id": "news-entity-view",
            }],
        }, timeout=60)
    response.raise_for_status()
    print("Data views created for entities and classified news")
    print("Visualize:", base + "/app/visualize#/edit/news-entity-top10")


if __name__ == "__main__":
    main()
