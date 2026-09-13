FROM apache/airflow:3.1.4

USER airflow

# duckdb is pinned explicitly: unpinned, dbt-duckdb resolves it to 1.5.x,
# which writes a warehouse the Superset image (duckdb 1.4.4) cannot read.
RUN pip install uv && uv pip install --no-cache-dir \
    dbt-core==1.10.15 \
    dbt-duckdb==1.10.0 \
    duckdb==1.4.4
