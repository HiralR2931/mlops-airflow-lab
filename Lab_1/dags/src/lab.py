"""
Data processing and modeling functions for the Airflow Lab 1 pipeline.

Change 1: functions read/write files in a per-run folder and return only
          small values (file paths, numbers). Large data never goes through XCom.
Change 2: validate_data() quality checks used by the branching task.
Change 4: split modeling into small steps; train the final model at the
          elbow k and scale test data with the saved scaler (bug fixes).
"""
import os
import re
import json
import pickle

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans
from kneed import KneeLocator

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .../dags
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

DEFAULT_FEATURES = ["BALANCE", "PURCHASES", "CREDIT_LIMIT"]
KMEANS_KWARGS = {"init": "random", "n_init": 10, "max_iter": 300, "random_state": 42}


# ---------- helpers ----------

def get_run_dir(run_id: str) -> str:
    """Create (if needed) and return a folder for this DAG run's files."""
    safe_id = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    run_dir = os.path.join(OUTPUT_DIR, safe_id)
    os.makedirs(run_dir, exist_ok=True)
    return run_dir


def _load_pickle(path: str):
    with open(path, "rb") as f:
        return pickle.load(f)


def _save_pickle(obj, path: str) -> str:
    with open(path, "wb") as f:
        pickle.dump(obj, f)
    return path


# ---------- data ----------

def load_data(run_dir: str) -> str:
    """Read the training CSV and save a copy for this run. Returns its path."""
    df = pd.read_csv(os.path.join(DATA_DIR, "file.csv"))
    raw_path = os.path.join(run_dir, "raw.pkl")
    df.to_pickle(raw_path)
    print(f"Loaded {len(df)} rows -> {raw_path}")
    return raw_path


def validate_data(raw_path: str, run_dir: str, features=None,
                  min_rows: int = 100, max_null_pct: float = 0.2) -> dict:
    """Data quality checks before training. Returns a report dict with 'ok' and 'issues'."""
    features = features or DEFAULT_FEATURES
    df = pd.read_pickle(raw_path)
    issues, usable_rows, null_pct = [], 0, {}

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
        "ok": not issues, "issues": issues,
        "total_rows": int(len(df)), "usable_rows": usable_rows,
        "null_pct": null_pct, "features": features, "min_rows": min_rows,
    }
    with open(os.path.join(run_dir, "data_quality_report.json"), "w") as f:
        json.dump(report, f, indent=2)

    print(f"Validation {'PASSED' if report['ok'] else 'FAILED'}: {report}")
    return report


def data_preprocessing(raw_path: str, run_dir: str, features=None) -> dict:
    """Drop missing rows, keep feature columns, MinMax-scale. Returns file paths."""
    features = features or DEFAULT_FEATURES
    df = pd.read_pickle(raw_path).dropna()

    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(df[features])

    scaled_path = _save_pickle(scaled, os.path.join(run_dir, "scaled.pkl"))
    scaler_path = _save_pickle(scaler, os.path.join(run_dir, "scaler.pkl"))

    print(f"Preprocessed {len(df)} rows using {features}")
    return {"scaled_path": scaled_path, "scaler_path": scaler_path, "rows": int(len(df))}


# ---------- modeling ----------

def compute_sse(scaled_path: str, max_k: int = 49) -> list:
    """Fit KMeans for k = 1..max_k and return the SSE (inertia) for each k."""
    X = _load_pickle(scaled_path)
    max_k = min(max_k, len(X))
    sse = [float(KMeans(n_clusters=k, **KMEANS_KWARGS).fit(X).inertia_)
           for k in range(1, max_k + 1)]
    print(f"Computed SSE for k=1..{max_k}")
    return sse


def find_best_k(sse: list, fallback: int = 3) -> int:
    """Elbow method. Falls back to a default if no clear elbow is found."""
    kl = KneeLocator(range(1, len(sse) + 1), sse, curve="convex", direction="decreasing")
    if kl.elbow is None:
        k = min(fallback, len(sse))
        print(f"No clear elbow found, using fallback k={k}")
        return k
    print(f"Optimal no. of clusters: {kl.elbow}")
    return int(kl.elbow)


def train_final_model(scaled_path: str, run_dir: str, k: int) -> str:
    """FIX: train the final model at the elbow k (original saved the k=49 model)."""
    X = _load_pickle(scaled_path)
    model = KMeans(n_clusters=k, **KMEANS_KWARGS).fit(X)
    model_path = _save_pickle(model, os.path.join(run_dir, "final_model.sav"))
    print(f"Trained final model with k={k} -> {model_path}")
    return model_path


def predict_test(model_path: str, scaler_path: str, run_dir: str, features=None) -> dict:
    """FIX: scale test data with the saved scaler before predicting."""
    features = features or DEFAULT_FEATURES
    model = _load_pickle(model_path)
    scaler = _load_pickle(scaler_path)

    test_df = pd.read_csv(os.path.join(DATA_DIR, "test.csv"))
    preds = model.predict(scaler.transform(test_df[features]))

    out = test_df.copy()
    out["cluster"] = preds
    pred_path = os.path.join(run_dir, "predictions.csv")
    out.to_csv(pred_path, index=False)

    print(f"Predicted {len(out)} test rows -> {pred_path}")
    return {"predictions_path": pred_path, "n_rows": int(len(out)),
            "first_prediction": int(preds[0])}


def write_report(model_path: str, scaler_path: str, run_dir: str,
                 sse: list, k: int, features=None) -> str:
    """Write a Markdown report: best k, cluster sizes and centers in real units."""
    features = features or DEFAULT_FEATURES
    model = _load_pickle(model_path)
    scaler = _load_pickle(scaler_path)

    sizes = np.bincount(model.labels_)
    centers = scaler.inverse_transform(model.cluster_centers_)

    lines = [
        "# K-Means Clustering Report", "",
        f"- **Best k (elbow method):** {k}",
        f"- **Features:** {', '.join(features)}",
        f"- **k values tried:** 1..{len(sse)}", "",
        "## Clusters (centers in original units)", "",
        "| Cluster | Size | " + " | ".join(features) + " |",
        "|---|---|" + "---|" * len(features),
    ]
    for i, (size, center) in enumerate(zip(sizes, centers)):
        lines.append(f"| {i} | {int(size)} | " + " | ".join(f"{v:,.2f}" for v in center) + " |")

    lines += ["", "## SSE by k", "", "| k | SSE |", "|---|---|"]
    lines += [f"| {i} | {v:,.4f} |" for i, v in enumerate(sse, start=1)]

    report_path = os.path.join(run_dir, "report.md")
    with open(report_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    print(f"Report written -> {report_path}")
    return report_path
