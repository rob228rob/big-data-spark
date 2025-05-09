# ETL pipeline: Spark -->> PostgreSQL -> ClickHouse витрины

## Запуск автоматизирован
```bash
# запускаем окружение: кликхаус, постгре, спарк
docker-compose up -d postgres-db clickhouse spark-master spark-worker

# запускаем локально скрипт load_mock_data.py для выгрузки данных из исходных в постгре
cd ETL/postgresql/
./load_mock_data.py

# любым удобным способом выполняем ddl из sql/clickhouse_ddl.sql
# (мне стало чуть лень это автоматизировать)

# запуск самой спарк джобы когда окружение с данными будет готовоо
docker-compose up spark-job --build

# опционально можно кильнуть все
docker-compose down # -v # если волюмы тоже нужно удалитб