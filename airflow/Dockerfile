FROM apache/airflow:3.1.4

USER airflow

COPY airflow/requirements.txt /tmp/requirements.txt

RUN pip install uv && uv pip install --no-cache-dir -r /tmp/requirements.txt
