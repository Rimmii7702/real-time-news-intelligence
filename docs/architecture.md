# Architecture and implementation approach

## Problem

News feeds arrive continuously, but readers usually need two derived views: **what topics are being covered** and **which named entities are appearing most often**. A batch-trained classifier supplies topic categories, while a stream processor and a search index keep the news and entity frequencies queryable.

## Data lifecycle

1. **Ingest:** `news_producer.py` polls two public BBC RSS endpoints. `feedparser` parses title, summary, link and publication metadata; `common.py` strips HTML and hashes the GUID/link to a stable `article_id`.
2. **Deduplicate:** SQLite stores each acknowledged article ID. It avoids repeats between polls; if the process fails after Kafka acknowledgment but before the SQLite commit, the record may be resent. No exactly-once claim is made.
3. **Buffer:** Kafka `news-raw` decouples feed polling from Spark. Two output topics separate entity updates and classified article events.
4. **NER:** Spark Python UDF calls spaCy `en_core_web_sm` inside workers, normalizes entity case/spacing, filters selected named-entity types and explodes mentions.
5. **Aggregate:** Spark Structured Streaming uses a stateful `groupBy(entity, entity_type).count()` with complete-mode updates. A `foreachBatch` sink writes cumulative counts to Kafka, each keyed by a stable hash of `(type, normalized entity)`.
6. **Classify:** A separate query inside the **same Spark application** broadcasts the trained AG News model to executors, tokenizes articles consistently, chooses the class with maximum multinomial NB log probability and writes to Kafka.
7. **Index:** Logstash consumes both output topics. Elasticsearch maps `category/entity/entity_type` as `keyword`, entity counts as `long`, and dates as `date`. Stable document IDs ensure idempotent indexing for repeated updates.
8. **Visualize:** Kibana displays top entities by `sum(count)` and categories by article document count. Old screenshot evidence is preserved independently of live measurements.

## Batch ML details

Input is the public **AG News train.csv** (`label_id`, `title`, `description`) with four classes. The driver validates the download, tokenizes articles, and parallelizes them as RDDs.

- Train/test: deterministic RDD `randomSplit([0.8, 0.2], seed=42)`; no test-set fitting or vocabulary leakage.
- Priors: `P(c) = training_documents_in_c / total_training_documents`.
- RDDs count `(class,token)` occurrences via `flatMap` and `reduceByKey`.
- Laplace smoothing: `P(token|class) = (count(class,token)+1)/(total_tokens_in_class+|V|)`.
- Prediction: choose the class maximizing `log P(class) + sum(log P(token|class))`.
- Unknown tokens receive `log(1/(total_tokens_in_class+|V|))`, matching the original classroom implementation.
- Evaluation: confusion counts via `map(actual,predicted)` + `reduceByKey`, then accuracy, precision, recall, macro and weighted F1 from the confusion matrix.
- Export: compressed JSON model; this project does not use Spark MLlib NaiveBayes or scikit-learn.

## Batch entity analytics

The optional Gutenberg job preserves an independent NLTK pipeline from the earlier assignment: `wget` public text → NLTK sentence tokenization + POS tagging + named-entity chunking → Spark `flatMap` → `(entity,1)` → `reduceByKey` → sorted top 25. Its output is not mixed with the live news feed.

## Semantics and trade-offs

| Area | Current choice | Limitation / next improvement |
|---|---|---|
| Input | BBC RSS title + summary | Not full news articles; RSS supply is low-volume |
| State | Spark cumulative NER counts | State increases without a watermark/window |
| Streaming | 30s microbatches, 1 Kafka partition | Demonstration of integration, not high-volume load test |
| Reliability | SQLite dedup, Kafka acknowledgment, ES upserts | At-least-once end-to-end; restart can replay |
| Classification | Multinomial NB trained on AG News | Domain shift to BBC requires a BBC-labeled sample |
| Evaluation | Held-out AG News test subset | Separate official AG News test set not used |
| Deployment | Local Docker Compose, no ES authentication | Development-only and localhost-bound |

## Possible improvements (not claimed as implemented)

Windowed entity trends; stronger news classifiers; BBC-labeled error analysis; Kafka partition scaling; separate serving and training deployments; CI integration tests; exactly-once-compatible sink processing; additional performance benchmarks with controlled input volume.
