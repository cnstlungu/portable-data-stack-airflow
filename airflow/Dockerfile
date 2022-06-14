FROM ednarb29/docker-airflow:2.2.3
RUN pip install xmltodict
USER root
RUN apt-get update
RUN apt-get install -y postgresql-client
USER airflow