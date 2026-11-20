"""Quickly classify your own headline with the saved AG News NB model."""
from __future__ import annotations
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from news_classifier import load_model, predict_text


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("text", help="Headline or brief news description")
    p.add_argument("--model", default="artifacts/naive_bayes_model.json.gz")
    a = p.parse_args()
    model = load_model(a.model)
    print("Predicted category:", predict_text(a.text, model))


if __name__ == "__main__":
    main()
