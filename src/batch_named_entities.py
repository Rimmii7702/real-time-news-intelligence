"""Optional batch NER/WordCount demo on a public-domain Gutenberg book.

Preserves the first project's NLTK -> Spark flatMap/map/reduceByKey approach.
This batch text is a separate benchmark, not the data used by the live dashboard.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import time

# Leo Tolstoy, War and Peace (Project Gutenberg eBook #2600)
DEFAULT_URL = "https://www.gutenberg.org/files/2600/2600-0.txt"


def download_nltk_resources() -> None:
    import nltk
    nltk_dir = os.getenv("NLTK_DATA", str(Path.home() / "nltk_data"))
    Path(nltk_dir).mkdir(parents=True, exist_ok=True)
    required = (
        "punkt", "punkt_tab", "averaged_perceptron_tagger_eng",
        "maxent_ne_chunker_tab", "words",
    )
    for package in required:
        if not nltk.download(package, download_dir=nltk_dir, quiet=True):
            raise RuntimeError(f"Could not download NLTK package: {package}")


def extract_entities(line: str) -> list[str]:
    import nltk
    found = []
    for sentence in nltk.sent_tokenize(line):
        tokens = nltk.word_tokenize(sentence)
        if not tokens:
            continue
        tagged = nltk.pos_tag(tokens)
        for node in nltk.ne_chunk(tagged, binary=False):
            if hasattr(node, "label"):
                name = " ".join(token for token, _ in node.leaves()).strip()
                if name:
                    found.append(name)
    return found


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--top", type=int, default=25)
    p.add_argument("--output", default="/workspace/metrics/batch_entity_counts.json")
    a = p.parse_args()
    if a.top <= 0:
        p.error("--top must be positive")

    from pyspark.sql import SparkSession
    download_nltk_resources()
    started = time.perf_counter()
    text = subprocess.check_output(["wget", "-qO-", a.url], text=True, encoding="utf-8", errors="ignore")
    spark = SparkSession.builder.appName("NewsIntelligence-BatchEntityWordCount").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        sc = spark.sparkContext
        lines = sc.parallelize(text.splitlines(), max(2, sc.defaultParallelism))
        # The same MapReduce pattern used for named entity frequency analysis.
        counts = (lines.flatMap(extract_entities)
                  .map(lambda ent: (ent, 1))
                  .reduceByKey(lambda a, b: a + b))
        top = counts.takeOrdered(a.top, key=lambda kv: (-kv[1], kv[0].lower()))
        print("\nTop named entities from Gutenberg corpus:")
        for entity, count in top:
            print(f"{entity:45.45s} {count}")
        output = Path(a.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps({
            "source": a.url, "method": "NLTK NER + Spark RDD MapReduce",
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "top_entities": [{"entity": entity, "count": count} for entity, count in top],
        }, indent=2) + "\n", encoding="utf-8")
        print("Saved:", output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
