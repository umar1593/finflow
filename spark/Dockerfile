FROM apache/spark:3.5.1

USER root

RUN curl -o /opt/spark/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

WORKDIR /app
COPY transform.py .

USER spark

CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--driver-memory", "1g", "/app/transform.py"]
