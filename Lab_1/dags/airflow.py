"""
Airflow Lab 1 - K-Means clustering pipeline.
Rewritten with the Airflow 3 TaskFlow API (@dag / @task).

Change 1: TaskFlow API + file paths through XCom (not data).
Change 2: data validation + branching (train only if data passes checks).
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task, get_current_context

from src import lab


def _run_dir() -> str:
    """Folder for the current DAG run's intermediate files."""
    return lab.get_run_dir(get_current_context()["run_id"])


@dag(
    dag_id="Airflow_Lab1",
    description="K-Means clustering pipeline with data validation and branching",
    start_date=datetime(2026, 1, 15),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "Hiral Rana",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["mlops", "lab1", "kmeans"],
)
def airflow_lab1():

    @task
    def load_data() -> str:
        return lab.load_data(_run_dir())

    @task
    def validate(raw_path: str) -> dict:
        return lab.validate_data(raw_path, _run_dir())

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
        return lab.data_preprocessing(raw_path, _run_dir())

    @task
    def build_model(prep: dict) -> dict:
        return lab.build_save_model(prep["scaled_path"], _run_dir())

    @task
    def evaluate(model_info: dict) -> int:
        return lab.load_model_elbow(model_info["model_path"], model_info["sse"])

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
