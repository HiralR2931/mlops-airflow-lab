"""
Airflow Lab 1 - K-Means clustering pipeline.
Rewritten with the Airflow 3 TaskFlow API (@dag / @task).

Change 1: TaskFlow API + file paths through XCom (not data).
Change 2: data validation + branching (train only if data passes checks).
Change 3: runtime params (set max_k, features, thresholds at trigger time).
Change 4: parallel fan-out / fan-in + ML fixes (elbow-k model, scaled test data).
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task, get_current_context, Param

from src import lab


def _run_dir() -> str:
    """Folder for the current DAG run's intermediate files."""
    return lab.get_run_dir(get_current_context()["run_id"])


def _params() -> dict:
    """Params chosen for this run (defaults, or values from the Trigger form)."""
    return get_current_context()["params"]


@dag(
    dag_id="Airflow_Lab1",
    description="K-Means clustering pipeline: validation, branching, params, parallel tasks",
    start_date=datetime(2026, 1, 15),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "Hiral Rana",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
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
    },
    tags=["mlops", "lab1", "kmeans"],
)
def airflow_lab1():

    # ---------- data ----------
    @task
    def load_data() -> str:
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
