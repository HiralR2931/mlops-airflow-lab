"""
Data processing and modeling functions for the Airflow Lab 1 pipeline.

Change 1: functions read/write files in a per-run folder and return only
small values (file paths, numbers). Large data never goes through XCom.
"""
import os
import re
import pickle

import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans
from kneed import KneeLocator

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .../dags
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

DEFAULT_FEATURES = ["BALANCE", "PURCHASES", "CREDIT_LIMIT"]
KMEANS_KWARGS = {"init": "random", "n_init": 10, "max_iter": 300, "random_state": 42}


def get_run_dir(run_id: str) -> str:
    """Create (if needed) and return a folder for this DAG run's files."""
    safe_id = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    run_dir = os.path.join(OUTPUT_DIR, safe_id)
    os.makedirs(run_dir, exist_ok=True)
    return run_dir


def load_data(run_dir: str) -> str:
    """Read the training CSV and save a copy for this run. Returns its path."""
    df = pd.read_csv(os.path.join(DATA_DIR, "file.csv"))
    raw_path = os.path.join(run_dir, "raw.pkl")
    df.to_pickle(raw_path)
    print(f"Loaded {len(df)} rows -> {raw_path}")
    return raw_path


def data_preprocessing(raw_path: str, run_dir: str, features=None) -> dict:
    """Drop missing rows, keep feature columns, MinMax-scale. Returns file paths."""
    features = features or DEFAULT_FEATURES
    df = pd.read_pickle(raw_path).dropna()

    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(df[features])

    scaled_path = os.path.join(run_dir, "scaled.pkl")
    scaler_path = os.path.join(run_dir, "scaler.pkl")
    with open(scaled_path, "wb") as f:
        pickle.dump(scaled, f)
    with open(scaler_path, "wb") as f:
        pickle.dump(scaler, f)

    print(f"Preprocessed {len(df)} rows using {features}")
    return {"scaled_path": scaled_path, "scaler_path": scaler_path, "rows": int(len(df))}


def build_save_model(scaled_path: str, run_dir: str, max_k: int = 49) -> dict:
    """Fit KMeans for k = 1..max_k, record SSE, save the last model."""
    with open(scaled_path, "rb") as f:
        X = pickle.load(f)

    sse = []
    for k in range(1, max_k + 1):
        kmeans = KMeans(n_clusters=k, **KMEANS_KWARGS).fit(X)
        sse.append(float(kmeans.inertia_))

    # Same behaviour as the original lab: saves the last-fitted model (k = max_k).
    model_path = os.path.join(run_dir, "model.sav")
    with open(model_path, "wb") as f:
        pickle.dump(kmeans, f)

    print(f"Trained k=1..{max_k}, saved model -> {model_path}")
    return {"model_path": model_path, "sse": sse}


def load_model_elbow(model_path: str, sse: list, features=None) -> int:
    """Find the elbow k and predict the cluster of the first test row."""
    with open(model_path, "rb") as f:
        model = pickle.load(f)

    kl = KneeLocator(range(1, len(sse) + 1), sse, curve="convex", direction="decreasing")
    print(f"Optimal no. of clusters: {kl.elbow}")

    test_df = pd.read_csv(os.path.join(DATA_DIR, "test.csv"))
    pred = int(model.predict(test_df[features or DEFAULT_FEATURES])[0])
    print(f"Predicted cluster for first test row: {pred}")
    return pred


import json


def validate_data(raw_path: str, run_dir: str, features=None,
                  min_rows: int = 100, max_null_pct: float = 0.2) -> dict:
    """
    Data quality checks before training. Returns a report dict:
    {"ok": bool, "issues": [...], "total_rows": n, "usable_rows": n, ...}
    """
    features = features or DEFAULT_FEATURES
    df = pd.read_pickle(raw_path)
    issues = []
    usable_rows = 0
    null_pct = {}

    missing = [c for c in features if c not in df.columns]
    if missing:
        issues.append(f"Missing columns: {missing}")
    else:
        non_numeric = [c for c in features if not pd.api.types.is_numeric_dtype(df[c])]
        if non_numeric:
            issues.append(f"Non-numeric columns: {non_numeric}")

        null_pct = {c: round(float(df[c].isna().mean()), 4) for c in features}
        too_null = {c: p for c, p in null_pct.items() if p > max_null_pct}
        if too_null:
            issues.append(f"Too many missing values (> {max_null_pct:.0%}): {too_null}")

        usable_rows = int(len(df.dropna()))
        if usable_rows < min_rows:
            issues.append(f"Only {usable_rows} usable rows (need at least {min_rows})")

    report = {
        "ok": not issues,
        "issues": issues,
        "total_rows": int(len(df)),
        "usable_rows": usable_rows,
        "null_pct": null_pct,
        "features": features,
        "min_rows": min_rows,
    }

    with open(os.path.join(run_dir, "data_quality_report.json"), "w") as f:
        json.dump(report, f, indent=2)

    print(f"Validation {'PASSED' if report['ok'] else 'FAILED'}: {report}")
    return report
