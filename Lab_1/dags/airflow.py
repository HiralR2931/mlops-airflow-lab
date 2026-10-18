"""
Airflow Lab 1 - K-Means clustering pipeline.
Rewritten with the Airflow 3 TaskFlow API (@dag / @task).

Change 1: TaskFlow API + file paths through XCom (not data).
Change 2: data validation + branching (train only if data passes checks).
Change 3: runtime params (set max_k, features, thresholds at trigger time).
Change 4: parallel fan-out / fan-in + ML fixes (elbow-k model, scaled test data).
Change 5: reliability - retries with backoff, timeouts, failure/retry callbacks,
          schedule, max one active run, and a simulate_failure param for testing.
"""
import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task, get_current_context, Param

from src import lab

log = logging.getLogger(__name__)
ALERTS_LOG = os.path.join(lab.OUTPUT_DIR, "alerts.log")


# ---------- callbacks (Change 5) ----------

def _log_event(context, event: str) -> None:
    """Append one JSON line per retry/failure to output/alerts.log."""
    ti = context["ti"]
    record = {
        "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "event": event,
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": ti.run_id,
        "try_number": ti.try_number,
        "error": str(context.get("exception")),
    }
    os.makedirs(lab.OUTPUT_DIR, exist_ok=True)
    with open(ALERTS_LOG, "a") as f:
        f.write(json.dumps(record) + "\n")
    log.warning("ALERT %s: %s", event, record)


def on_task_retry(context) -> None:
    _log_event(context, "RETRY")


def on_task_failure(context) -> None:
    _log_event(context, "FAILED")


# ---------- helpers ----------

def _run_dir() -> str:
    """Folder for the current DAG run's intermediate files."""
    return lab.get_run_dir(get_current_context()["run_id"])


def _params() -> dict:
    """Params chosen for this run (defaults, or values from the Trigger form)."""
    return get_current_context()["params"]


@dag(
    dag_id="Airflow_Lab1",
    description="K-Means clustering pipeline: validation, branching, params, parallel tasks, retries",
    start_date=datetime(2026, 1, 15),
    schedule="0 6 * * *",                 # daily at 06:00 UTC (manual triggers still work)
    catchup=False,
    max_active_runs=1,                    # runs never overlap
    dagrun_timeout=timedelta(minutes=30), # whole run must finish in 30 min
    default_args={
        "owner": "Hiral Rana",
        "retries": 2,
        "retry_delay": timedelta(seconds=15),
        "retry_exponential_backoff": True,      # 15s, then ~30s, ...
        "max_retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=10),
        "on_retry_callback": on_task_retry,
        "on_failure_callback": on_task_failure,
    },
    params={
        "max_k": Param(49, type="integer", minimum=2, maximum=100,
                       description="Try k = 1..max_k clusters"),
        "features": Param(lab.DEFAULT_FEATURES, type="array", items={"type": "string"},
                          description="Columns to cluster on"),
        "min_rows": Param(100, type="integer", minimum=1,
                          description="Validation: minimum usable rows"),
        "max_null_pct": Param(0.2, type="number", minimum=0, maximum=1,
                              description="Validation: max share of missing values per column"),
        "simulate_failure": Param(False, type="boolean",
                                  description="Testing: make load_data fail on its first attempt"),
    },
    tags=["mlops", "lab1", "kmeans"],
)
def airflow_lab1():

    # ---------- data ----------
    @task
    def load_data() -> str:
        ctx = get_current_context()
        if ctx["params"]["simulate_failure"] and ctx["ti"].try_number == 1:
            raise RuntimeError("Simulated failure on first attempt (testing retries)")
        return lab.load_data(_run_dir())

    @task
    def validate(raw_path: str) -> dict:
        p = _params()
        return lab.validate_data(raw_path, _run_dir(), features=p["features"],
                                 min_rows=p["min_rows"], max_null_pct=p["max_null_pct"])

    @task.branch
    def check_quality(report: dict) -> str:
        """Return the task_id to run next. The other path is skipped."""
        if report["ok"]:
            print("Data passed validation -> preprocess")
            return "preprocess"
        print("Data failed validation -> report_bad_data")
        return "report_bad_data"

    @task
    def report_bad_data(report: dict) -> None:
        print("Pipeline stopped before training. Issues found:")
        for issue in report["issues"]:
            print(f"  - {issue}")

    @task
    def preprocess(raw_path: str) -> dict:
        return lab.data_preprocessing(raw_path, _run_dir(), features=_params()["features"])

    # ---------- modeling ----------
    @task
    def compute_sse(prep: dict) -> list:
        return lab.compute_sse(prep["scaled_path"], max_k=_params()["max_k"])

    @task
    def find_best_k(sse: list) -> int:
        return lab.find_best_k(sse)

    @task
    def train_final_model(prep: dict, k: int) -> str:
        return lab.train_final_model(prep["scaled_path"], _run_dir(), k)

    # ---------- parallel outputs (fan-out) ----------
    @task
    def predict_test(model_path: str, prep: dict) -> dict:
        return lab.predict_test(model_path, prep["scaler_path"], _run_dir(),
                                features=_params()["features"])

    @task
    def write_report(model_path: str, prep: dict, sse: list, k: int) -> str:
        return lab.write_report(model_path, prep["scaler_path"], _run_dir(),
                                sse, k, features=_params()["features"])

    # ---------- join (fan-in) ----------
    @task
    def summary(preds: dict, report_path: str, k: int) -> dict:
        result = {
            "best_k": k,
            "test_rows_predicted": preds["n_rows"],
            "first_test_prediction": preds["first_prediction"],
            "predictions_file": preds["predictions_path"],
            "report_file": report_path,
        }
        print("=== Pipeline summary ===")
        for key, value in result.items():
            print(f"{key}: {value}")
        return result

    # ---- Wiring ----
    raw = load_data()
    report = validate(raw)
    decision = check_quality(report)

    prep = preprocess(raw)
    bad = report_bad_data(report)
    decision >> [prep, bad]          # branch: only one path runs

    sse = compute_sse(prep)
    k = find_best_k(sse)
    model_path = train_final_model(prep, k)

    preds = predict_test(model_path, prep)               # these two run
    report_path = write_report(model_path, prep, sse, k)  # in parallel

    summary(preds, report_path, k)                       # waits for both


dag = airflow_lab1()

if __name__ == "__main__":
    dag.test()
