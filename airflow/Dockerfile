FROM ednarb29/docker-airflow:2.2.3
RUN pip install xmltodict
RUN pip install airflow-dbt
RUN pip install dbt-postgres==1.0.2
COPY airflow/profiles.yml /usr/local/airflow/.dbt/profiles.yml


USER root
RUN apt-get update
RUN apt-get install -y postgresql-client
USER airflow