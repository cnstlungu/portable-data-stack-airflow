FROM ednarb29/docker-airflow:2.2.3
 
COPY airflow/profiles.yml /usr/local/airflow/.dbt/profiles.yml

USER root
RUN apt-get update
RUN apt-get install -y postgresql-client wget

RUN pip install uv

RUN uv pip install --system xmltodict==0.13.0 airflow-dbt==0.4.0 dbt-postgres==1.0.2

USER airflow