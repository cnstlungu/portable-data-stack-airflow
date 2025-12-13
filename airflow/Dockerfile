FROM apache/airflow:3.1.4

USER airflow

RUN pip install uv

RUN uv pip install --no-cache-dir dbt-core==1.10.15 dbt-duckdb==1.10.0
