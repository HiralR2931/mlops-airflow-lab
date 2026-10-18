"""
Airflow Lab 1 - K-Means clustering pipeline.
Rewritten with the Airflow 3 TaskFlow API (@dag / @task).

Change 1: TaskFlow API + file paths through XCom (not data).
Change 2: data validation + branching (train only if data passes checks).
Change 3: runtime params (set max_k, features, thresholds at trigger time).
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
    description="K-Means clustering pipeline with validation, branching and runtime params",
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

    @task
    def load_data() -> str:
        return lab.load_data(_run_dir())

    @task
    def validate(raw_path: str) -> dict:
        p = _params()
        return lab.validate_data(
            raw_path, _run_dir(),
            features=p["features"],
            min_rows=p["min_rows"],
            max_null_pct=p["max_null_pct"],
        )

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

    @task
    def build_model(prep: dict) -> dict:
        return lab.build_save_model(prep["scaled_path"], _run_dir(), max_k=_params()["max_k"])

    @task
    def evaluate(model_info: dict) -> int:
        return lab.load_model_elbow(
            model_info["model_path"], model_info["sse"], features=_params()["features"]
        )

    # ---- Wiring ----
    raw = load_data()
    report = validate(raw)
    decision = check_quality(report)

    prep = preprocess(raw)
    bad = report_bad_data(report)

    # The branch picks ONE of these two paths
    decision >> [prep, bad]

    evaluate(build_model(prep))


dag = airflow_lab1()

if __name__ == "__main__":
    dag.test()
