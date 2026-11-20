"""Kafka -> Spark Structured Streaming -> entity counts + classified news articles.

The NER / cumulative-count path is adapted from the original Structured Streaming
implementation. Classification is an added project integration using the AG News
Multinomial Naive Bayes model trained by src/train_classifier.py.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time

from kafka import KafkaProducer
from pyspark.sql import SparkSession, functions as F, types as T
from news_classifier import load_model, predict_text
from common import entity_key

_NLP_MODELS = {}


def find_entities(text: str | None, model_name: str) -> list[tuple[str, str]]:
    """Run spaCy NER in Spark Python workers; normalize names and retain types."""
    if not text:
        return []
    if model_name not in _NLP_MODELS:
        import spacy
        _NLP_MODELS[model_name] = spacy.load(
            model_name, disable=["tagger", "parser", "lemmatizer", "attribute_ruler"]
        )
    doc = _NLP_MODELS[model_name](text[:12000])
    result = []
    for ent in doc.ents:
        if ent.label_ not in {"PERSON", "ORG", "GPE", "LOC", "FAC", "EVENT", "PRODUCT", "NORP"}:
            continue
        label = re.sub(r"\s+", " ", ent.text).strip().casefold()
        if len(label) >= 2:
            result.append((label, ent.label_))
    return result


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default="kafka:29092")
    p.add_argument("--input-topic", default="news-raw")
    p.add_argument("--entity-topic", default="entity-counts")
    p.add_argument("--classified-topic", default="article-insights")
    p.add_argument("--checkpoint", default="/workspace/run/spark-checkpoints")
    p.add_argument("--metrics-path", default="/workspace/run/stream_progress.jsonl")
    p.add_argument("--model", default="/workspace/artifacts/naive_bayes_model.json.gz")
    p.add_argument("--skip-classification", action="store_true", help="Run the original NER-only stream")
    p.add_argument("--trigger", default="30 seconds")
    p.add_argument("--ner-model", default="en_core_web_sm")
    p.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    a = p.parse_args()

    checkpoint_root = Path(a.checkpoint)
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    metrics_path = Path(a.metrics_path)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    if not a.skip_classification:
        model = load_model(a.model)  # fail early if model has not been trained

    spark = (SparkSession.builder.appName("NewsIntelligence-Streaming")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    schema = T.StructType([
        T.StructField("article_id", T.StringType()),
        T.StructField("title", T.StringType()),
        T.StructField("text", T.StringType()),
        T.StructField("url", T.StringType()),
        T.StructField("published_at", T.StringType()),
        T.StructField("feed_url", T.StringType()),
        T.StructField("observed_at", T.StringType()),
    ])
    entity_schema = T.ArrayType(T.StructType([
        T.StructField("entity", T.StringType()),
        T.StructField("entity_type", T.StringType()),
    ]))
    extract_udf = F.udf(lambda text: find_entities(text, a.ner_model), entity_schema)

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", a.bootstrap)
           .option("subscribe", a.input_topic)
           .option("startingOffsets", a.starting_offsets)
           .option("failOnDataLoss", "true")
           .load()
           .select(F.from_json(F.col("value").cast("string"), schema).alias("article"))
           .select("article.*")
           .where(F.col("article_id").isNotNull() & F.col("text").isNotNull()))

    # Stateful, cumulative entity mentions. Output is the full current snapshot;
    # Logstash uses the stable entity_id as the Elasticsearch document ID.
    mentions = (raw.select("text")
                .withColumn("entities", extract_udf("text"))
                .select(F.explode("entities").alias("entity"))
                .select("entity.entity", "entity.entity_type"))
    counts = mentions.groupBy("entity", "entity_type").count()

    def publish_entity_counts(batch, batch_id: int) -> None:
        producer = KafkaProducer(
            bootstrap_servers=a.bootstrap,
            value_serializer=lambda item: json.dumps(item, ensure_ascii=False).encode("utf-8"),
            acks="all", retries=5, max_block_ms=30000,
        )
        stamp = datetime.now(timezone.utc).isoformat()
        published = 0
        try:
            for row in batch.toLocalIterator():
                document = {
                    "entity_id": entity_key(row["entity"], row["entity_type"]),
                    "entity": row["entity"],
                    "entity_type": row["entity_type"],
                    "count": int(row["count"]),
                    "batch_id": int(batch_id),
                    "updated_at": stamp,
                }
                producer.send(
                    a.entity_topic, key=document["entity_id"].encode(), value=document
                ).get(timeout=30)
                published += 1
            producer.flush(timeout=60)
        finally:
            producer.close()
        print(f"NER microbatch {batch_id}: published {published} current counts", flush=True)

    query_ner = (counts.writeStream.queryName("ner_counts")
                 .outputMode("complete")
                 .foreachBatch(publish_entity_counts)
                 .option("checkpointLocation", str(checkpoint_root / "ner_counts"))
                 .trigger(processingTime=a.trigger)
                 .start())
    queries = [query_ner]

    if not a.skip_classification:
        model_bc = spark.sparkContext.broadcast(model)
        category_udf = F.udf(lambda text: predict_text(text, model_bc.value), T.StringType())
        articles = (raw.withColumn("category", category_udf(F.col("text")))
                    .withColumn("classification_model", F.lit("AG News Multinomial NB"))
                    .withColumn("processed_at", F.date_format(
                        F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                    )))
        payload_cols = [
            "article_id", "title", "text", "url", "published_at", "feed_url",
            "observed_at", "processed_at", "category", "classification_model",
        ]
        kafka_messages = articles.select(
            F.col("article_id").alias("key"),
            F.to_json(F.struct(*[F.col(col) for col in payload_cols])).alias("value"),
        )
        query_classification = (kafka_messages.writeStream.queryName("news_classification")
            .format("kafka")
            .option("kafka.bootstrap.servers", a.bootstrap)
            .option("topic", a.classified_topic)
            .option("checkpointLocation", str(checkpoint_root / "news_classification"))
            .outputMode("append")
            .trigger(processingTime=a.trigger)
            .start())
        queries.append(query_classification)

    print(f"News intelligence stream started. Queries: {[q.name for q in queries]}", flush=True)
    last_batch_ids = {}
    try:
        while all(q.isActive for q in queries):
            time.sleep(5)
            for query in queries:
                progress = query.lastProgress
                if not progress:
                    continue
                batch_id = progress.get("batchId")
                if last_batch_ids.get(query.name) == batch_id:
                    continue
                last_batch_ids[query.name] = batch_id
                entry = {
                    "query": query.name,
                    "batchId": batch_id,
                    "timestamp": progress.get("timestamp"),
                    "numInputRows": progress.get("numInputRows"),
                    "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
                    "processedRowsPerSecond": progress.get("processedRowsPerSecond"),
                    "durationMs": progress.get("durationMs", {}),
                }
                with metrics_path.open("a", encoding="utf-8") as output:
                    output.write(json.dumps(entry) + "\n")
                print(f"[{query.name}] batch={batch_id} input={entry['numInputRows']} "
                      f"rate={float(entry['processedRowsPerSecond'] or 0):.2f} rows/s", flush=True)
        for query in queries:
            if query.exception() is not None:
                raise RuntimeError(f"Query {query.name} failed: {query.exception()}")
    except KeyboardInterrupt:
        print("Stopping live streams", flush=True)
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
