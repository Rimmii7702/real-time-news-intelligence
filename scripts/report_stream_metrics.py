"""Summarize real Spark Structured Streaming progress logs (no fake benchmark)."""
from __future__ import annotations
import argparse
from datetime import datetime
import json
from pathlib import Path
from statistics import mean, median


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * pct / 100
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def summarize(records: list[dict], query: str) -> dict:
    rows = [r for r in records if r.get("query") == query]
    if not rows:
        raise ValueError(f"No progress records for query {query!r}")
    # Progress for the classification query counts *news input articles*.
    # Do not add it to the NER query's rows: those are another read of the same input.
    rates = [float(r.get("processedRowsPerSecond") or 0) for r in rows]
    durations = [float((r.get("durationMs") or {}).get("triggerExecution") or 0) for r in rows]
    timestamps = []
    for r in rows:
        try:
            timestamps.append(datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00")))
        except (ValueError, KeyError, TypeError):
            continue
    window = (max(timestamps) - min(timestamps)).total_seconds() if len(timestamps) >= 2 else None
    return {
        "query": query,
        "source": "Spark StreamingQuery.lastProgress (observed, not simulated)",
        "observed_microbatches": len(rows),
        "input_rows_across_observed_batches": sum(int(r.get("numInputRows") or 0) for r in rows),
        "mean_reported_processing_rate_rows_per_second": round(mean(rates), 3),
        "peak_reported_processing_rate_rows_per_second": round(max(rates), 3),
        "median_batch_execution_ms": round(median(durations), 2),
        "p95_batch_execution_ms": round(percentile(durations, 95), 2),
        "observation_window_seconds": round(window, 1) if window is not None else None,
        "note": "Spark processedRowsPerSecond is batch processing rate, NOT sustained wall-clock feed throughput or end-to-end latency.",
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--progress", default="run/stream_progress.jsonl")
    p.add_argument("--query", default="news_classification", choices=["ner_counts", "news_classification"])
    p.add_argument("--output", default="metrics/stream_performance.json")
    a = p.parse_args()
    rows = [json.loads(line) for line in Path(a.progress).read_text(encoding="utf-8").splitlines() if line.strip()]
    summary = summarize(rows, a.query)
    target = Path(a.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
