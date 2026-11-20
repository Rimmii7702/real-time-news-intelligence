from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from common import article_key, entity_key, normalize_entity, plain_text
from report_stream_metrics import percentile, summarize


def test_html_cleaning():
    assert plain_text("<p>A &amp; B</p>  <b>news</b>") == "A & B news"


def test_article_dedup_key():
    assert article_key({"link": "https://example.com/x"}) == article_key({"link": "https://example.com/x"})
    assert article_key({"link": "https://example.com/x"}) != article_key({"link": "https://example.com/y"})


def test_entity_normalization_and_id():
    assert normalize_entity("  New   York ") == "new york"
    assert entity_key("New York", "GPE") == entity_key(" new    YORK ", "GPE")
    assert entity_key("New York", "GPE") != entity_key("New York", "ORG")


def test_stream_report_uses_only_one_query():
    rows = [
        {"query": "news_classification", "numInputRows": 10, "processedRowsPerSecond": 2.0,
         "durationMs": {"triggerExecution": 500}, "timestamp": "2026-11-01T10:00:00Z"},
        {"query": "news_classification", "numInputRows": 5, "processedRowsPerSecond": 4.0,
         "durationMs": {"triggerExecution": 300}, "timestamp": "2026-11-01T10:00:30Z"},
        {"query": "ner_counts", "numInputRows": 15, "processedRowsPerSecond": 8.0,
         "durationMs": {"triggerExecution": 200}, "timestamp": "2026-11-01T10:00:30Z"},
    ]
    summary = summarize(rows, "news_classification")
    assert summary["input_rows_across_observed_batches"] == 15
    assert summary["mean_reported_processing_rate_rows_per_second"] == 3.0
    assert summary["observation_window_seconds"] == 30.0
    assert percentile([10, 20, 30, 40], 50) == 25
