FROM bitnami/spark:3.5

USER root

RUN mkdir -p /opt/spark/jars && \
    curl -o /opt/spark/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

WORKDIR /app
COPY transform.py .

USER 1001
