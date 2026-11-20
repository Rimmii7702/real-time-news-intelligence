from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from news_classifier import (
    tokenize, predict_text, classification_metrics, save_model, load_model
)


def mini_model():
    return {
        "labels": ["business", "sports"],
        "log_priors": {"business": 0.0, "sports": 0.0},
        "log_token_probabilities": {
            "business": {"stocks": -0.1, "team": -5.0},
            "sports": {"stocks": -5.0, "team": -0.1},
        },
        "unknown_log_probabilities": {"business": -2.0, "sports": -2.0},
    }


def test_preprocessing():
    assert tokenize("The Team wins AND stocks rise! 123") == ["team", "wins", "stocks", "rise"]


def test_predicts_expected_label():
    model = mini_model()
    assert predict_text("team team", model) == "sports"
    assert predict_text("stocks stocks", model) == "business"


def test_serialization_round_trip(tmp_path):
    target = tmp_path / "model.json.gz"
    save_model(mini_model(), target)
    assert load_model(target) == mini_model()


def test_metrics_from_confusion_matrix():
    confusion = {
        ("world", "world"): 8,
        ("world", "sports"): 2,
        ("sports", "world"): 1,
        ("sports", "sports"): 9,
    }
    scores = classification_metrics(confusion, ["world", "sports"])
    assert scores["accuracy"] == pytest.approx(0.85)
    assert scores["evaluated_articles"] == 20
    assert scores["per_class"]["world"]["recall"] == pytest.approx(0.8)
    assert scores["per_class"]["sports"]["precision"] == pytest.approx(9 / 11)
