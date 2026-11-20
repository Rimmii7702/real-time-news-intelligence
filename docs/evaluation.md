# Evaluation, performance and resume metrics

## 1. Classification accuracy: evaluate on labeled AG News

A previously completed standalone run measured **90.29% accuracy** (21,650/23,979) and **0.9025 macro-F1** on a held-out subset of AG News; see `results/archived_ag_news_baseline.json`. The integrated project's current numbers are generated in `metrics/classifier_metrics.json` by running `src/train_classifier.py` (see root README). Do not present the historical score as a new BBC evaluation. The script uses the **original AG News training CSV**, a seeded 80/20 train/test split and no precomputed test results.

| Metric | Meaning | Calculated as |
|---|---|---|
| Accuracy | Correct classification fraction | correct predictions / labeled test articles |
| Precision (per class) | Of predicted class `c`, fraction actually in `c` | TP / (TP+FP) |
| Recall (per class) | Of true class `c`, fraction found | TP / (TP+FN) |
| F1 (per class) | Balanced precision and recall | 2PR / (P+R) |
| Macro F1 | Equally weights four categories | average of the four class F1 values |
| Weighted F1 | Weights classes by test support | support-weighted average of class F1 |
| Training time | Wall-clock fitting time | monotonic timer around MapReduce model fitting |
| Evaluation rate | Test predictions per wall-clock second | test articles / test-scoring seconds |

**Never substitute an NER mention count or live-news topic distribution for classifier accuracy.** AG News test-set accuracy does not measure performance on BBC live news.

### Commands

```bash
docker compose exec -T runtime spark-submit --master 'local[2]' --driver-memory 3g src/train_classifier.py
docker compose exec -T runtime cat metrics/classifier_metrics.json
```

Inspect the exported `accuracy`, `macro_f1`, `weighted_f1`, `test_articles`, `seed`, and runtime fields before reporting any number. The dataset's canonical separate test.csv was not used; don't describe this as official benchmark test accuracy.

## 2. Stream execution performance

Spark's query progress events are written to `run/stream_progress.jsonl` (timestamp, input rows, processed rows/second and microbatch duration). Summarize only **`news_classification`**, not the sum of both queries, because both read the same raw articles.

```bash
docker compose exec -T runtime python scripts/report_stream_metrics.py --query news_classification
```

Reports observed microbatches, total input rows in observed batches, mean and peak **Spark-reported batch processing rate**, and median/p95 batch execution time. These metrics are useful for profiling but **are not the maximum system throughput or end-to-end delivery speed**. A low-volume RSS feed cannot establish a stress-test rate.

## 3. Indexed article and latency audit

```bash
docker compose exec -T runtime python scripts/audit_live_index.py --es-url http://elasticsearch:9200
```

Reports indexed articles, named-entity mention sum, category counts, and p50/p95 time difference from the original `observed_at` to the Spark-produced `processed_at` timestamp (a **processing-lag proxy**). It excludes the later Kafka→Logstash→Elasticsearch indexing time. Do not claim full end-to-end latency from this number.

## 4. Optional independently labeled BBC quality audit

To support a **BBC-specific accuracy** claim, sample e.g. 100 unique BBC feed items after collection, hide the predicted labels, have a human assign each to one of `world/sports/business/sci_tech`, and evaluate only when labels are unambiguous. Keep labeling policy and sample size. Many mixed-category headlines cannot be mapped perfectly to AG News categories, so allow an `ambiguous` review bucket and exclude those only under a declared policy.

## Resume bullets: fill measured values, never guesses

**Data Engineer / Big Data Engineer**

> Built a real-time news intelligence pipeline using Kafka, PySpark Structured Streaming, spaCy, Elasticsearch, and Kibana to ingest RSS feeds, classify news articles, and maintain cumulative named-entity analytics; processed **[measured article count]** items during a **[measured duration]** run.

**ML / NLP**

> Implemented a four-class Multinomial Naive Bayes text classifier using Spark RDD MapReduce and Laplace smoothing; achieved **[actual accuracy]%** accuracy and **[actual macro F1]** macro-F1 on a seeded held-out subset of AG News (**[test sample count]** articles).

**Streaming performance**

> Instrumented Spark Structured Streaming microbatches and Elasticsearch indexing; recorded **[measured rate]** Spark-reported processed rows/s and **[measured p95]** ms p95 batch execution time under **[documented workload]**.

Only add measured figures once the pipeline is actually run. Keep the test dataset, volume, and method attached to each result.
