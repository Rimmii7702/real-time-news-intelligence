"""Train and evaluate a four-class AG News Multinomial Naive Bayes using Spark RDDs.

Retains the original project approach: tokenize -> MapReduce class/term counts ->
Laplace-smoothed log probabilities -> test-set predictions -> confusion matrix.
Use a fixed seeded 80/20 split of the 120k-row AG News *training* CSV.
This is not the official separate AG News test set.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
from operator import add
from pathlib import Path
import time
import urllib.request

from news_classifier import (
    CLASS_NAMES, tokenize, predict_tokens, save_model, classification_metrics
)

DATASET_URL = "https://raw.githubusercontent.com/mhjabreel/CharCnn_Keras/master/data/ag_news_csv/train.csv"
DATASET_MD5 = "b1a00f826fdfbd249f79597b59e1dc12"  # pinned in original assignment implementation


def get_records(source: str) -> tuple[list[tuple[str, list[str]]], str]:
    """Download once on driver, validate bytes and parse title+description."""
    if source.startswith(("http://", "https://")):
        req = urllib.request.Request(source, headers={"User-Agent": "NewsIntelligence-Portfolio/1.0"})
        with urllib.request.urlopen(req, timeout=90) as response:
            payload = response.read()
    else:
        payload = Path(source).read_bytes()
    digest = hashlib.md5(payload).hexdigest()
    if source == DATASET_URL and digest != DATASET_MD5:
        raise ValueError(f"AG News checksum mismatch: expected {DATASET_MD5}, got {digest}")
    records = []
    for row in csv.reader(io.StringIO(payload.decode("utf-8-sig", errors="replace"))):
        if len(row) != 3:
            continue
        try:
            label = CLASS_NAMES[int(row[0])]
        except (ValueError, KeyError):
            continue
        text = f"{row[1]} {row[2]}"
        records.append((label, tokenize(text)))
    if not records:
        raise ValueError("No valid labeled rows were parsed from the AG News CSV")
    return records, digest


def fit_naive_bayes(train_rdd) -> dict:
    """Derive document priors and word likelihoods entirely with Spark RDDs."""
    class_docs = train_rdd.map(lambda pair: (pair[0], 1)).reduceByKey(add).collectAsMap()
    num_docs = sum(class_docs.values())
    labels = sorted(class_docs)
    if set(labels) != set(CLASS_NAMES.values()):
        raise ValueError("Expected examples from all four classes in training partition")

    # Each word occurrence contributes one count for its (class, token).
    class_term_counts = train_rdd.flatMap(
        lambda row: [((row[0], word), 1) for word in row[1]]
    ).reduceByKey(add).cache()
    vocabulary_size = class_term_counts.map(lambda kv: kv[0][1]).distinct().count()
    if vocabulary_size == 0:
        raise ValueError("Empty vocabulary")
    totals = dict(class_term_counts.map(
        lambda kv: (kv[0][0], kv[1])
    ).reduceByKey(add).collect())
    class_terms = {label: {} for label in labels}
    for (label, word), count in class_term_counts.toLocalIterator():
        class_terms[label][word] = math.log((count + 1) / (totals[label] + vocabulary_size))
    class_term_counts.unpersist()
    return {
        "format_version": 1,
        "algorithm": "Spark RDD Multinomial Naive Bayes (add-one smoothing)",
        "labels": labels,
        "vocabulary_size": vocabulary_size,
        "class_document_counts": dict(class_docs),
        "log_priors": {label: math.log(class_docs[label] / num_docs) for label in labels},
        "unknown_log_probabilities": {
            label: math.log(1 / (totals[label] + vocabulary_size)) for label in labels
        },
        "log_token_probabilities": class_terms,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=DATASET_URL, help="AG News train.csv local path or URL")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--test-fraction", type=float, default=0.2)
    parser.add_argument("--model-out", default="/workspace/artifacts/naive_bayes_model.json.gz")
    parser.add_argument("--metrics-out", default="/workspace/metrics/classifier_metrics.json")
    args = parser.parse_args()
    if not 0.05 <= args.test_fraction <= 0.5:
        parser.error("test-fraction must be between 0.05 and 0.50")

    # Importing PySpark only here keeps shared inference unit-testable without Spark.
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("NewsIntelligence-AGNews-NaiveBayes").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    started = time.perf_counter()
    try:
        records, md5 = get_records(args.source)
        sc = spark.sparkContext
        examples = sc.parallelize(records, max(sc.defaultParallelism, 2))
        train, test = examples.randomSplit([1 - args.test_fraction, args.test_fraction], seed=args.seed)
        train, test = train.cache(), test.cache()
        train_count, test_count = train.count(), test.count()
        if not train_count or not test_count:
            raise ValueError("Seeded train/test split produced an empty subset")

        fit_start = time.perf_counter()
        model = fit_naive_bayes(train)
        train_seconds = time.perf_counter() - fit_start
        broadcast_model = sc.broadcast(model)
        score_start = time.perf_counter()
        confusion = test.map(
            lambda row: ((row[0], predict_tokens(row[1], broadcast_model.value)), 1)
        ).reduceByKey(add).collectAsMap()
        score_seconds = time.perf_counter() - score_start
        scores = classification_metrics(confusion, model["labels"])
        scores.update({
            "dataset": "AG News training CSV (title + description)",
            "dataset_source": args.source,
            "dataset_md5": md5,
            "split": "seeded random 80/20 partition of training CSV" if args.test_fraction == 0.2 else "seeded random partition of training CSV",
            "seed": args.seed,
            "test_fraction": args.test_fraction,
            "total_articles": len(records),
            "training_articles": train_count,
            "test_articles": test_count,
            "vocabulary_size": model["vocabulary_size"],
            "fit_seconds": round(train_seconds, 3),
            "evaluation_seconds": round(score_seconds, 3),
            "evaluation_articles_per_second": round(test_count / score_seconds, 3) if score_seconds else None,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "note": "Accuracy/F1 refer only to the held-out labeled AG News subset, not live BBC articles or NER.",
        })
        save_model(model, args.model_out)
        output = Path(args.metrics_out)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(scores, indent=2) + "\n", encoding="utf-8")
        print("\nTraining complete; model saved:", args.model_out)
        print("Evaluation metrics saved:", output)
        print(f"Train: {train_count:,} | Test: {test_count:,} | Vocabulary: {model['vocabulary_size']:,}")
        print(f"Held-out accuracy: {scores['accuracy'] * 100:.2f}%")
        print(f"Macro F1: {scores['macro_f1']:.4f} | Weighted F1: {scores['weighted_f1']:.4f}")
        print(f"Fit: {train_seconds:.1f}s | Test scoring: {score_seconds:.1f}s")
        print("Per-class metrics and confusion matrix are in the metrics JSON.")
        broadcast_model.unpersist()
        train.unpersist()
        test.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
