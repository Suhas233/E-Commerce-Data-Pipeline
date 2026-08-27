```python
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.databricks.hooks.databricks import DatabricksHook


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}


@dag(
    dag_id="ecommerce_databricks_pipeline",
    default_args=default_args,
    description="E-commerce Medallion pipeline using Databricks",
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
    tags=["ecommerce", "databricks", "medallion"]
)
def ecommerce_pipeline():

    @task
    def bronze_layer():
        hook = DatabricksHook(
            databricks_conn_id="databricks_default"
        )

        run = hook.run_now(
            job_id=101
        )

        return run["run_id"]


    @task
    def silver_layer(bronze_run_id):
        hook = DatabricksHook(
            databricks_conn_id="databricks_default"
        )

        run = hook.run_now(
            job_id=102
        )

        return run["run_id"]


    @task
    def gold_layer(silver_run_id):
        hook = DatabricksHook(
            databricks_conn_id="databricks_default"
        )

        run = hook.run_now(
            job_id=103
        )

        return run["run_id"]


    bronze = bronze_layer()

    silver = silver_layer(bronze)

    gold = gold_layer(silver)


ecommerce_pipeline()
```
