"""Phase 1 bootstrap: keep the project DAG paused, including existing databases."""

from airflow.models import DagModel
from airflow.utils.session import create_session

# airflow-init gọi script này sau khi khởi tạo metadata của Airflow.
# Session tự commit khi thành công, rollback nếu có lỗi.
with create_session() as session:
    dag = (
        session.query(DagModel)
        .filter_by(dag_id="pm25_forecast_pipeline")
        .one_or_none()
    )

    # Áp dụng cả cho DAG đã tồn tại; DAG mới dùng is_paused_upon_creation=True.
    if dag is not None:
        dag.is_paused = True
        print("Phase 1: pm25_forecast_pipeline is paused.")
