"""
Minimal PySpark job that connects to the warehouse (Postgres) via JDBC.
Use from the repo root:
  docker compose run --rm pyspark spark-submit /app/example_job.py
Candidates can add their own scripts in this directory and submit the same way.
"""
from pyspark.sql import SparkSession

# Warehouse connection (same network as docker-compose; use service name as host)
jdbc_url = "jdbc:postgresql://warehouse:5432/warehouse"
connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

spark = SparkSession.builder.appName("example_job").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Example: read a table (create one first if needed) or run a simple query via JDBC
# This reads the public schema's tables list; replace with your own query/table.
df = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT 1 AS id) AS stub",
    properties=connection_properties,
)
df.show()

spark.stop()
