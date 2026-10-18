"""
Airflow Lab 1 - K-Means clustering pipeline.
Rewritten with the Airflow 3 TaskFlow API (@dag / @task).
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task, get_current_context

from src import lab


def _run_dir() -> str:
    """Folder for the current DAG run's intermediate files."""
    return lab.get_run_dir(get_current_context()["run_id"])


@dag(
    dag_id="Airflow_Lab1",
    description="K-Means clustering pipeline (TaskFlow API version)",
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
    def preprocess(raw_path: str) -> dict:
        return lab.data_preprocessing(raw_path, _run_dir())

    @task
    def build_model(prep: dict) -> dict:
        return lab.build_save_model(prep["scaled_path"], _run_dir())

    @task
    def evaluate(model_info: dict) -> int:
        return lab.load_model_elbow(model_info["model_path"], model_info["sse"])

    # Calling the tasks like functions sets the order automatically:
    # load_data -> preprocess -> build_model -> evaluate
    evaluate(build_model(preprocess(load_data())))


dag = airflow_lab1()

if __name__ == "__main__":
    dag.test()
