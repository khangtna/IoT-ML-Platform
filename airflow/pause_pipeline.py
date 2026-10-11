"""Phase 1 bootstrap: keep the project DAG paused, including existing databases."""
from airflow.models import DagModel
from airflow.utils.session import create_session

with create_session() as session:
    dag = session.query(DagModel).filter_by(dag_id="pm25_forecast_pipeline").one_or_none()
    if dag is not None:
        dag.is_paused = True
        print("Phase 1: pm25_forecast_pipeline is paused.")
