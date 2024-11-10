FROM python:3.9-slim

RUN apt-get update && apt-get install -y wget

ARG POSTGRES_USER
ARG POSTGRES_PASSWORD

ENV POSTGRES_USER=$POSTGRES_USER
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD

WORKDIR /home/generator

RUN mkdir csv

RUN mkdir xml

COPY generator .

RUN pip install uv

RUN uv pip install --system -r requirements.txt

CMD ["python", "generate.py"]