FROM apache/superset
ARG SUPERSET_ADMIN
ARG SUPERSET_PASSWORD
ARG POSTGRES_USER
ARG POSTGRES_PASSWORD
# Switching to root to install the required packages
USER root
RUN pip install psycopg2-binary
USER superset
RUN superset fab create-admin \
              --username ${SUPERSET_ADMIN} \
              --firstname Superset \
              --lastname Admin \
              --email admin@superset.com \
              --password ${SUPERSET_PASSWORD}
RUN superset db upgrade
RUN superset init
RUN superset set_database_uri -d OLTP  -u postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@oltp:5432/sales_oltp
RUN superset set_database_uri -d DW  -u postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@dw:5432/sales_dw
COPY superset/dashboards.json .
RUN superset import_dashboards -p dashboards.json
