import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as sum_, avg as avg_, col, count

PG_DATABASE      = os.getenv("PG_DATABASE", "main_db")
POSTGRE_HOST     = os.getenv("PG_HOST",     "bigdata-db")
POSTGRE_PORT     = int(os.getenv("PG_PORT",   "5432"))
POSTGRE_USER     = os.getenv("PG_USER",     "bigdata")
POSTGRE_PASSWORD = os.getenv("PG_PASSWORD", "bigdata")
CH_HOST          = os.getenv("CH_HOST",     "clickhouse-server")
CH_PORT          = os.getenv("CH_PORT",     "8123")
CH_DATABASE      = os.getenv("CH_DATABASE", "analytics")
CH_DRIVER        = os.getenv("CH_DRIVER",   "com.clickhouse.jdbc.ClickHouseDriver")

JARS = os.getenv(
    "SPARK_JARS",
    "/opt/bitnami/spark/jars/clickhouse-jdbc-0.8.3-all.jar,"
    "/opt/bitnami/spark/jars/postgresql-42.7.3.jar"
)

# JDBC URLs
jdbc_conf = {
    'pg.url'   : f"jdbc:postgresql://{POSTGRE_HOST}:{POSTGRE_PORT}/{PG_DATABASE}",
    'pg.user'  : POSTGRE_USER,
    'pg.password': POSTGRE_PASSWORD,
    'ch.url'   : f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DATABASE}?user=default&password=",
    'ch.driver': CH_DRIVER
}
pg_url   = jdbc_conf['pg.url']
pg_props = {
    'user':     jdbc_conf['pg.user'],
    'password': jdbc_conf['pg.password'],
    'driver':   'org.postgresql.Driver'
}
ch_url   = jdbc_conf['ch.url']
ch_props = {
    'driver': jdbc_conf['ch.driver']
}

spark = (
    SparkSession.builder
        .appName('BuildReports')
        .config('spark.jars', JARS)
        .config("spark.sql.catalog.clickhouse.host",     CH_HOST)
        .config("spark.sql.catalog.clickhouse.protocol", "http")
        .config("spark.sql.catalog.clickhouse.http_port",CH_PORT)
        .config("spark.sql.catalog.clickhouse.database", CH_DATABASE)
        .getOrCreate()
)

sales      = spark.read.jdbc(pg_url, 'facts_sales',            properties=pg_props)
products   = spark.read.jdbc(pg_url, 'dim_products',           properties=pg_props)
categories = spark.read.jdbc(pg_url, 'dim_product_categories', properties=pg_props)
customers  = spark.read.jdbc(pg_url, 'dim_customers',          properties=pg_props)
dates      = spark.read.jdbc(pg_url, 'dim_dates',              properties=pg_props)
stores     = spark.read.jdbc(pg_url, 'dim_stores',             properties=pg_props)
suppliers  = spark.read.jdbc(pg_url, 'dim_suppliers',          properties=pg_props)
countries  = (
    spark.read.jdbc(pg_url, 'dim_countries', properties=pg_props)
         .withColumnRenamed('name', 'country')  # id оставляем как есть
)

# Витрина продаж по продуктам ---
sales_prod = (
    sales
    .join(products,   sales.product_id   == products.id)
    .join(categories, products.category_id == categories.id)
    .groupBy(
        products.id.alias('product_id'),
        products.name.alias('product_name'),
        categories.name.alias('category')
    )
    .agg(
        sum_('total_price').alias('total_revenue'),
        sum_('product_quantity').alias('total_quantity'),
        avg_('rating').alias('avg_rating'),
        sum_('reviews').alias('review_count')
    )
)
sales_prod.write.format('jdbc')\
    .option('url',    ch_url)\
    .option('dbtable','analytics.sales_by_product')\
    .options(**ch_props)\
    .mode("append")\
    .save()

#  Витрина продаж по клиентам ---
cust = (
    sales
    .join(customers, sales.customer_id == customers.id)
    .join(dates,     sales.date_id     == dates.id)
)
sales_cust = (
    cust
    .groupBy('customer_id','first_name','last_name','country_id')
    .agg(
        sum_('total_price').alias('total_spent'),
        avg_('total_price').alias('avg_order_value')
    )
    .join(countries, col('country_id') == countries.id, how='left')
    .selectExpr(
        'customer_id',
        "concat(first_name,' ',last_name) as customer_name",
        'country',
        'total_spent',
        'avg_order_value'
    )
)
sales_cust.write.format('jdbc')\
    .option('url',    ch_url)\
    .option('dbtable','analytics.sales_by_customer')\
    .options(**ch_props)\
    .mode("append")\
    .save()

#  Витрина продаж по времени ---
reports_time = (
    sales
    .join(dates, sales.date_id == dates.id)
    .withColumn('year',  col('date').substr(1,4).cast('int'))
    .withColumn('month', col('date').substr(6,2).cast('int'))
    .groupBy('year','month')
    .agg(
        sum_('total_price').alias('total_revenue'),
        count('*').alias('total_orders')           # вместо sum('id')
    )
    .withColumn('avg_order_size', col('total_revenue')/col('total_orders'))
)
reports_time.write.format('jdbc')\
    .option('url',    ch_url)\
    .option('dbtable','analytics.sales_by_time')\
    .options(**ch_props)\
    .mode("append")\
    .save()

#  Витрина продаж по магазинам ---
rep_store = (
    sales
    .join(stores, sales.store_id == stores.id)
    .join(dates,  sales.date_id  == dates.id)
    .groupBy('store_id','name','location','country_id')
    .agg(
        sum_('total_price').alias('total_revenue'),
        count('*').alias('total_orders')             # вместо sum('id')
    )
    .withColumn('avg_order_value', col('total_revenue')/col('total_orders'))
    .join(countries, col('country_id') == countries.id, how='left')
    .selectExpr(
        'store_id',
        'name   as store_name',
        'location as city',
        'country',
        'total_revenue',
        'avg_order_value'
    )
)
rep_store.write.format('jdbc')\
    .option('url',    ch_url)\
    .option('dbtable','analytics.sales_by_store')\
    .options(**ch_props)\
    .mode("append")\
    .save()

#  Витрина продаж по поставщикам ---
rep_sup = (
    sales
    .join(suppliers, sales.supplier_id == suppliers.id)
    .groupBy('supplier_id','name','country_id')
    .agg(
        sum_('total_price').alias('total_revenue'),
        avg_('product_quantity').alias('avg_price')
    )
    .join(countries, col('country_id') == countries.id, how='left')
    .selectExpr(
        'supplier_id',
        'name   as supplier_name',
        'country',
        'total_revenue',
        'avg_price'
    )
)
rep_sup.write.format('jdbc')\
    .option('url',    ch_url)\
    .option('dbtable','analytics.sales_by_supplier')\
    .options(**ch_props)\
    .mode("append")\
    .save()

#  Витрина качества продукции ---
rep_qual = (
    sales
    .join(products, sales.product_id == products.id)
    .groupBy('product_id', products.name.alias('product_name'), 'rating')
    .agg(
        sum_('reviews').alias('review_count'),
        sum_('product_quantity').alias('total_quantity')
    )
)
rep_qual.write.format('jdbc')\
    .option('url',    ch_url)\
    .option('dbtable','analytics.product_quality')\
    .options(**ch_props)\
    .mode("append")\
    .save()

spark.stop()
