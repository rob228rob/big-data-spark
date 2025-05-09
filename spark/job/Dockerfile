FROM bitnami/spark:3.5
USER root
RUN apt-get update && apt-get install -y curl
# Скачиваем JDBC и Spark-runtime JAR'ы
RUN curl -L -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
 && curl -L -o /opt/bitnami/spark/jars/clickhouse-jdbc-0.8.3-all.jar \
      https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.8.3/clickhouse-jdbc-0.8.3-all.jar

WORKDIR /app
COPY ETL/spark_jobs/build_reports.py ETL/spark_jobs/build_reports.py
ENTRYPOINT ["spark-submit","--master","spark://spark-master:7077","--deploy-mode","client","--jars","/opt/bitnami/spark/jars/clickhouse-jdbc-0.8.3-all.jar,/opt/bitnami/spark/jars/postgresql-42.7.3.jar","ETL/spark_jobs/build_reports.py"]
