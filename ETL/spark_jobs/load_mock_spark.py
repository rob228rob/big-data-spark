import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DecimalType,
    FloatType
)

spark = (
    SparkSession.builder
    .appName("LoadMockDataToPostgres")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4")
    .getOrCreate()
)

pg_host     = os.getenv("PG_HOST", "postgres")
pg_port     = os.getenv("PG_PORT", "5432")
pg_db       = os.getenv("PG_DATABASE", "main_db")
pg_user     = os.getenv("PG_USER", "bigdata")
pg_password = os.getenv("PG_PASSWORD", "bigdata")

jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
connection_props = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

schema = StructType([
    StructField("id",                   LongType(),    True),
    StructField("customer_first_name",  StringType(),  True),
    StructField("customer_last_name",   StringType(),  True),
    StructField("customer_age",         LongType(),    True),
    StructField("customer_email",       StringType(),  True),
    StructField("customer_country",     StringType(),  True),
    StructField("customer_postal_code", StringType(),  True),
    StructField("customer_pet_type",    StringType(),  True),
    StructField("customer_pet_name",    StringType(),  True),
    StructField("customer_pet_breed",   StringType(),  True),
    StructField("seller_first_name",    StringType(),  True),
    StructField("seller_last_name",     StringType(),  True),
    StructField("seller_email",         StringType(),  True),
    StructField("seller_country",       StringType(),  True),
    StructField("seller_postal_code",   StringType(),  True),
    StructField("product_name",         StringType(),  True),
    StructField("product_category",     StringType(),  True),
    StructField("product_price",        DecimalType(12,2), True),
    StructField("product_quantity",     LongType(),    True),
    StructField("sale_date",            StringType(),  True),
    StructField("sale_customer_id",     LongType(),    True),
    StructField("sale_seller_id",       LongType(),    True),
    StructField("sale_product_id",      LongType(),    True),
    StructField("sale_quantity",        LongType(),    True),
    StructField("sale_total_price",     DecimalType(14,2), True),
    StructField("store_name",           StringType(),  True),
    StructField("store_location",       StringType(),  True),
    StructField("store_city",           StringType(),  True),
    StructField("store_state",          StringType(),  True),
    StructField("store_country",        StringType(),  True),
    StructField("store_phone",          StringType(),  True),
    StructField("store_email",          StringType(),  True),
    StructField("pet_category",         StringType(),  True),
    StructField("product_weight",       FloatType(),   True),
    StructField("product_color",        StringType(),  True),
    StructField("product_size",         StringType(),  True),
    StructField("product_brand",        StringType(),  True),
    StructField("product_material",     StringType(),  True),
    StructField("product_description",  StringType(),  True),
    StructField("product_rating",       FloatType(),   True),
    StructField("product_reviews",      LongType(),    True),
    StructField("product_release_date", StringType(),  True),
    StructField("product_expiry_date",  StringType(),  True),
    StructField("supplier_name",        StringType(),  True),
    StructField("supplier_contact",     StringType(),  True),
    StructField("supplier_email",       StringType(),  True),
    StructField("supplier_phone",       StringType(),  True),
    StructField("supplier_address",     StringType(),  True),
    StructField("supplier_city",        StringType(),  True),
    StructField("supplier_country",     StringType(),  True),
])

data_path = "/opt/source_data/*.csv"
df = (
    spark.read
         .schema(schema)
         .option("header", True)
         .option("delimiter", ",")
         .csv(data_path)
)

df.write \
  .mode("append") \
  .jdbc(url=jdbc_url, table="public.mock_data", properties=connection_props)

print(f"[ok] Loaded {df.count()} rows into mock_data")

spark.stop()