# ETL pipeline: Spark -->> PostgreSQL -> ClickHouse витрины

## Запуск автоматизирован
```bash
# запускаем окружение: кликхаус, постгре, спарк
docker-compose up -d postgres-db clickhouse spark-master spark-worker

# любым удобным способом
docker-compose up spark-job-postgres --build

# запуск самой спарк джобы когда окружение с данными будет готовоо
docker-compose up spark-job-click --build

# опционально можно кильнуть все
docker-compose down # -v # если волюмы тоже нужно удалитб