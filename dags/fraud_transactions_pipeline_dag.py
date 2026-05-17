from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_PATH = "/opt/airflow"


with DAG(
    dag_id="fraud_transactions_pipeline",
    description="Pipeline de processamento de transações de fraude",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["localiza", "pyspark", "delta", "data-quality", "analytics", "fraud-transactions"],
) as dag:

    landing = BashOperator(
        task_id="landing",
        bash_command=f"cd {PROJECT_PATH} && python -m fraud_transactions.jobs.ingestion.landing_job",
    )

    raw = BashOperator(
        task_id="raw",
        bash_command=f"cd {PROJECT_PATH} && python -m fraud_transactions.jobs.ingestion.raw_job",
    )

    curated = BashOperator(
        task_id="curated",
        bash_command=f"cd {PROJECT_PATH} && python -m fraud_transactions.jobs.ingestion.curated_job",
    )

    avg_risk_score_by_region = BashOperator(
        task_id="avg_risk_score_by_region",
        bash_command=f"cd {PROJECT_PATH} && python -m fraud_transactions.jobs.analytics.avg_risk_score_by_region_job",
    )

    top3_receiving_address_by_latest_sale_amount = BashOperator(
        task_id="top3_receiving_address_by_latest_sale_amount",
        bash_command=f"cd {PROJECT_PATH} && python -m fraud_transactions.jobs.analytics.top3_receiving_address_job",
    )

    data_quality = BashOperator(
        task_id="data_quality",
        bash_command=f"cd {PROJECT_PATH} && python -m fraud_transactions.data_quality.data_quality_job",
    )

    landing >> raw >> curated
    curated >> [
        avg_risk_score_by_region,
        top3_receiving_address_by_latest_sale_amount,
    ] >> data_quality