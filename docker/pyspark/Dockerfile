# Extend Apache Spark (PySpark) with Postgres JDBC driver so candidates can
# run spark-submit /app/<script>.py without --packages.
FROM apache/spark-py:latest

USER root
RUN curl -sSfL -o /opt/spark/jars/postgresql-42.7.9.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.9.jar
COPY log4j2.properties /opt/spark/conf/log4j2.properties
ENV PATH="/opt/spark/bin:${PATH}" \
    SPARK_CLASSPATH="/opt/spark/conf::/opt/spark/jars/*"
# Quiet entrypoint (no shell trace); base image entrypoint is verbose.
ENTRYPOINT ["/usr/bin/tini", "-s", "--"]
