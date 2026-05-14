FROM apache/airflow:2.9.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN curl -sSL -o /opt/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
       "dbt-core==1.8.8" "dbt-postgres==1.8.8"

RUN python -m venv /opt/gx-venv \
    && /opt/gx-venv/bin/pip install --no-cache-dir \
       "great-expectations==0.18.22" psycopg2-binary "sqlalchemy<2.0" pandas

USER airflow

RUN pip install --no-cache-dir "pyspark==3.5.1" psycopg2-binary
