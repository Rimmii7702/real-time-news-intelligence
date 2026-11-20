"""Shared pure-Python preprocessing, inference, and model serialization.

The training algorithm lives in train_classifier.py and uses Spark RDD MapReduce.
Neither inference nor metrics requires scikit-learn.
"""
from __future__ import annotations

import gzip
import json
import math
import re
from pathlib import Path
from typing import Mapping

CLASS_NAMES = {1: "world", 2: "sports", 3: "business", 4: "sci_tech"}
STOP_WORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "in",
    "is", "it", "of", "on", "or", "that", "the", "to", "was", "were", "with",
    "you", "your", "i", "me", "my", "we", "our", "he", "she", "they", "them",
    "this", "these", "those", "am", "will",
}
TOKEN_RE = re.compile(r"[a-z]+")


def tokenize(text: str | None) -> list[str]:
    """Reproduce the training-time lowercase/stopword tokenizer at inference."""
    return [w for w in TOKEN_RE.findall((text or "").lower()) if w not in STOP_WORDS]


def predict_tokens(tokens: list[str], model: Mapping) -> str:
    """Multinomial NB: argmax_c log P(c) + sum_t log P(t|c)."""
    scores = {}
    for label in model["labels"]:
        score = model["log_priors"][label]
        seen = model["log_token_probabilities"][label]
        unseen_logp = model["unknown_log_probabilities"][label]
        for word in tokens:
            score += seen.get(word, unseen_logp)
        scores[label] = score
    return max(model["labels"], key=lambda label: scores[label])


def predict_text(text: str | None, model: Mapping) -> str:
    return predict_tokens(tokenize(text), model)


def save_model(model: dict, filepath: str | Path) -> None:
    target = Path(filepath)
    target.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(target, "wt", encoding="utf-8", compresslevel=6) as stream:
        json.dump(model, stream, separators=(",", ":"), ensure_ascii=False)


def load_model(filepath: str | Path) -> dict:
    with gzip.open(filepath, "rt", encoding="utf-8") as stream:
        model = json.load(stream)
    expected = {"labels", "log_priors", "log_token_probabilities", "unknown_log_probabilities"}
    if not expected.issubset(model):
        raise ValueError("Model file missing required Naive Bayes keys")
    return model


def classification_metrics(confusion: Mapping[tuple[str, str], int], labels: list[str]) -> dict:
    """Exact multi-class accuracy, precision, recall, F1 from actual/predicted counts."""
    per_class = {}
    total = sum(confusion.values())
    correct = sum(confusion.get((label, label), 0) for label in labels)
    for label in labels:
        tp = confusion.get((label, label), 0)
        predicted_count = sum(confusion.get((actual, label), 0) for actual in labels)
        support = sum(confusion.get((label, pred), 0) for pred in labels)
        precision = tp / predicted_count if predicted_count else 0.0
        recall = tp / support if support else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        per_class[label] = {
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "support": support,
        }
    macro_f1 = sum(x["f1"] for x in per_class.values()) / len(labels)
    weighted_f1 = sum(x["f1"] * x["support"] for x in per_class.values()) / total if total else 0.0
    return {
        "accuracy": correct / total if total else 0.0,
        "macro_f1": macro_f1,
        "weighted_f1": weighted_f1,
        "correct_predictions": correct,
        "evaluated_articles": total,
        "per_class": per_class,
        "confusion_matrix": {
            actual: {pred: int(confusion.get((actual, pred), 0)) for pred in labels}
            for actual in labels
        },
    }
