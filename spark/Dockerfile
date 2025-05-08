FROM bitnami/spark:3.5
USER root
RUN apt-get update && apt-get install -y curl
# PostgreSQL JAR
RUN curl -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
# ClickHouse JDBC
RUN curl -o /opt/bitnami/spark/jars/clickhouse-jdbc-0.7.3.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.7.3/clickhouse-jdbc-0.7.3.jar

USER root
CMD ["/opt/bitnami/spark/sbin/start-master.sh"]