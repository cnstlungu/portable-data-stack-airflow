FROM ednarb29/docker-airflow:2.2.3
 
COPY airflow/profiles.yml /usr/local/airflow/.dbt/profiles.yml

USER root
RUN apt-get update
RUN apt-get install -y postgresql-client wget

ADD --chmod=755 https://astral.sh/uv/install.sh /install.sh
RUN /install.sh && rm /install.sh

RUN /root/.cargo/bin/uv pip install --system xmltodict==0.13.0 airflow-dbt==0.4.0 dbt-postgres==1.0.2

USER airflow