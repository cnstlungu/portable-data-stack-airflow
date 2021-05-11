FROM puckel/docker-airflow:1.10.9
RUN pip install xmltodict
USER root
RUN apt-get update
RUN apt-get install -y postgresql-client
USER airflow