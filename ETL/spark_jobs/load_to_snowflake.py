import os

from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
        .appName("Spark_ETL_Postgres")
        .config("spark.jars", "../../jars/postgresql-42.7.3.jar")
        .getOrCreate()
)

pg_host     = os.getenv("PG_HOST", "postgres")
pg_port     = os.getenv("PG_PORT", "5432")
pg_db       = os.getenv("PG_DATABASE", "main_db")
pg_user     = os.getenv("PG_USER", "bigdata")
pg_password = os.getenv("PG_PASSWORD", "bigdata")

jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
conn_props = {"user": pg_user, "password": pg_password, "driver": "org.postgresql.Driver"}

raw_df = (
    spark.read
         .jdbc(jdbc_url, "public.mock_data", properties=conn_props)
)

# Countries
df_countries = (
    raw_df.select(F.col("customer_country").alias("name"))
         .union(raw_df.select(F.col("seller_country").alias("name")))
         .union(raw_df.select(F.col("store_country").alias("name")))
         .union(raw_df.select(F.col("supplier_country").alias("name")))
         .filter(F.col("name").isNotNull())
         .distinct()
)
df_countries.write.jdbc(jdbc_url, "public.dim_countries", mode="overwrite", properties=conn_props)

# Cities
df_cities = (
    raw_df.select(F.col("store_city").alias("name"))
         .union(raw_df.select(F.col("supplier_city").alias("name")))
         .filter(F.col("name").isNotNull())
         .distinct()
)
df_cities.write.jdbc(jdbc_url, "public.dim_cities", mode="overwrite", properties=conn_props)

# Dates (sale, release, expiry)
dates = (
    raw_df.select(F.to_date("sale_date", "MM/dd/yyyy").alias("date"))
         .union(raw_df.select(F.to_date("product_release_date", "MM/dd/yyyy").alias("date")))
         .union(raw_df.select(F.to_date("product_expiry_date", "MM/dd/yyyy").alias("date")))
         .filter(F.col("date").isNotNull())
         .distinct()
)
dates.write.jdbc(jdbc_url, "public.dim_dates", mode="overwrite", properties=conn_props)

# Pet types, breeds, categories
df_pet_types = raw_df.select(F.col("customer_pet_type").alias("name")).where(F.col("name").isNotNull()).distinct()
df_pet_breeds = raw_df.select(F.col("customer_pet_breed").alias("name")).where(F.col("name").isNotNull()).distinct()
df_pet_cats   = raw_df.select(F.col("pet_category").alias("name")).where(F.col("name").isNotNull()).distinct()

df_pet_types.write.jdbc(jdbc_url, "public.dim_pet_types", mode="overwrite", properties=conn_props)
df_pet_breeds.write.jdbc(jdbc_url, "public.dim_pet_breeds", mode="overwrite", properties=conn_props)
df_pet_cats.write.jdbc(jdbc_url, "public.dim_pet_categories", mode="overwrite", properties=conn_props)

# Products categories, colors, sizes, brands, materials
df_prod_cat   = raw_df.select(F.col("product_category").alias("name")).where(F.col("name").isNotNull()).distinct()
df_prod_color = raw_df.select(F.col("product_color").alias("name")).where(F.col("name").isNotNull()).distinct()
df_prod_size  = raw_df.select(F.col("product_size").alias("name")).where(F.col("name").isNotNull()).distinct()
df_prod_brand = raw_df.select(F.col("product_brand").alias("name")).where(F.col("name").isNotNull()).distinct()
df_prod_mat   = raw_df.select(F.col("product_material").alias("name")).where(F.col("name").isNotNull()).distinct()

df_prod_cat.write.jdbc(jdbc_url, "public.dim_product_categories", mode="overwrite", properties=conn_props)
df_prod_color.write.jdbc(jdbc_url, "public.dim_product_colors", mode="overwrite", properties=conn_props)
df_prod_size.write.jdbc(jdbc_url, "public.dim_product_sizes", mode="overwrite", properties=conn_props)
df_prod_brand.write.jdbc(jdbc_url, "public.dim_product_brands", mode="overwrite", properties=conn_props)
df_prod_mat.write.jdbc(jdbc_url, "public.dim_product_verbose", mode="overwrite", properties=conn_props)

dim_countries = spark.read.jdbc(jdbc_url, "public.dim_countries", properties=conn_props)
dim_cities    = spark.read.jdbc(jdbc_url, "public.dim_cities", properties=conn_props)
dim_dates     = spark.read.jdbc(jdbc_url, "public.dim_dates", properties=conn_props)
pet_types     = spark.read.jdbc(jdbc_url, "public.dim_pet_types", properties=conn_props)
pet_breeds    = spark.read.jdbc(jdbc_url, "public.dim_pet_breeds", properties=conn_props)
pet_cats      = spark.read.jdbc(jdbc_url, "public.dim_pet_categories", properties=conn_props)
prod_cats     = spark.read.jdbc(jdbc_url, "public.dim_product_categories", properties=conn_props)
prod_colors   = spark.read.jdbc(jdbc_url, "public.dim_product_colors", properties=conn_props)
prod_sizes    = spark.read.jdbc(jdbc_url, "public.dim_product_sizes", properties=conn_props)
prod_brands   = spark.read.jdbc(jdbc_url, "public.dim_product_brands", properties=conn_props)
prod_mat      = spark.read.jdbc(jdbc_url, "public.dim_product_verbose", properties=conn_props)


# Pets
df_pets = (
    raw_df.join(pet_types, raw_df.customer_pet_type == pet_types.name, "left")
         .join(pet_breeds, raw_df.customer_pet_breed == pet_breeds.name, "left")
         .join(pet_cats, raw_df.pet_category == pet_cats.name, "left")
         .select(
             F.col("id").alias("src_id"),
             F.col("customer_pet_name").alias("name"),
             pet_types.id.alias("type_id"),
             pet_breeds.id.alias("breed_id"),
             pet_cats.id.alias("category_id")
         )
         .dropDuplicates(["type_id", "name", "breed_id", "category_id"] )
)
df_pets.write.jdbc(jdbc_url, "public.dim_pets", mode="overwrite", properties=conn_props)

# Products
df_products = (
    raw_df.join(prod_cats, raw_df.product_category == prod_cats.name, "left")
         .join(prod_colors, raw_df.product_color == prod_colors.name, "left")
         .join(prod_sizes, raw_df.product_size == prod_sizes.name, "left")
         .join(prod_brands, raw_df.product_brand == prod_brands.name, "left")
         .join(prod_mat, raw_df.product_material == prod_mat.name, "left")
         .join(dim_dates.withColumnRenamed("id", "rel_id").withColumnRenamed("date","rel_date"), F.to_date(raw_df.product_release_date,"MM/dd/yyyy")==F.col("rel_date"), "left")
         .join(dim_dates.withColumnRenamed("id", "exp_id").withColumnRenamed("date","exp_date"), F.to_date(raw_df.product_expiry_date,"MM/dd/yyyy")==F.col("exp_date"), "left")
         .select(
             raw_df.product_name.alias("name"),
             prod_cats.id.alias("category_id"),
             raw_df.product_price.alias("price"),
             raw_df.product_weight.alias("weight"),
             prod_colors.id.alias("color_id"),
             prod_sizes.id.alias("size_id"),
             prod_brands.id.alias("brand_id"),
             prod_mat.id.alias("material_id"),
             raw_df.product_description.alias("description"),
             raw_df.product_rating.alias("rating"),
             raw_df.product_reviews.alias("reviews"),
             F.col("rel_id").alias("release_date_id"),
             F.col("exp_id").alias("expiry_date_id")
         )
         .dropDuplicates()
)
df_products.write.jdbc(jdbc_url, "public.dim_products", mode="overwrite", properties=conn_props)

# Customers
df_customers = (
    raw_df.join(dim_countries.withColumnRenamed("id","ctry_id"), raw_df.customer_country==F.col("name"),"left")
         .join(df_pets.select("src_id","name"), raw_df.id==F.col("src_id"),"left")
         .select(
             raw_df.customer_first_name.alias("first_name"),
             raw_df.customer_last_name.alias("last_name"),
             raw_df.customer_age.alias("age"),
             raw_df.customer_email.alias("email"),
             F.col("ctry_id").alias("country_id"),
             raw_df.customer_postal_code.alias("postal_code"),
             F.col("src_id").alias("pet_id")
         )
         .dropDuplicates()
)
df_customers.write.jdbc(jdbc_url, "public.dim_customers", mode="overwrite", properties=conn_props)

# Sellers
df_sellers = (
    raw_df.join(dim_countries.withColumnRenamed("id","ctry_id"), raw_df.seller_country==F.col("name"),"left")
         .select(
             raw_df.seller_first_name.alias("first_name"),
             raw_df.seller_last_name.alias("last_name"),
             raw_df.seller_email.alias("email"),
             F.col("ctry_id").alias("country_id"),
             raw_df.seller_postal_code.alias("postal_code")
         )
         .dropDuplicates()
)
df_sellers.write.jdbc(jdbc_url, "public.dim_sellers", mode="overwrite", properties=conn_props)

# Stores
df_stores = (
    raw_df.join(dim_countries.withColumnRenamed("id","ctry_id"), raw_df.store_country==F.col("name"),"left")
         .join(dim_cities.withColumnRenamed("id","cty_id"), raw_df.store_city==F.col("name"),"left")
         .select(
             raw_df.store_name.alias("name"),
             raw_df.store_location.alias("location"),
             F.col("cty_id").alias("city_id"),
             raw_df.store_state.alias("state"),
             F.col("ctry_id").alias("country_id"),
             raw_df.store_phone.alias("phone"),
             raw_df.store_email.alias("email")
         )
         .dropDuplicates()
)
df_stores.write.jdbc(jdbc_url, "public.dim_stores", mode="overwrite", properties=conn_props)

# Suppliers
df_suppliers = (
    raw_df.join(dim_countries.withColumnRenamed("id","ctry_id"), raw_df.supplier_country==F.col("name"),"left")
         .join(dim_cities.withColumnRenamed("id","cty_id"), raw_df.supplier_city==F.col("name"),"left")
         .select(
             raw_df.supplier_name.alias("name"),
             raw_df.supplier_contact.alias("contact"),
             raw_df.supplier_email.alias("email"),
             raw_df.supplier_phone.alias("phone"),
             raw_df.supplier_address.alias("address"),
             F.col("cty_id").alias("city_id"),
             F.col("ctry_id").alias("country_id")
         )
         .dropDuplicates()
)
df_suppliers.write.jdbc(jdbc_url, "public.dim_suppliers", mode="overwrite", properties=conn_props)

# 7) Facts: sales
df_facts = (
    raw_df.alias("m")
         .join(df_customers.select("email","id"), F.col("m.customer_email")==F.col("email"),"left").withColumnRenamed("id","customer_id")
         .join(df_sellers.select("email","id"), F.col("m.seller_email")==F.col("email"),"left").withColumnRenamed("id","seller_id")
         .join(df_products.select("name","id"), F.col("m.product_name")==F.col("name"),"left").withColumnRenamed("id","product_id")
         .join(dim_dates.withColumnRenamed("id","date_id"), F.to_date(F.col("m.sale_date"),"MM/dd/yyyy")==F.col("date"),"left")
         .join(df_stores.select("email","id"), F.col("m.store_email")==F.col("email"),"left").withColumnRenamed("id","store_id")
         .join(df_suppliers.select("email","id"), F.col("m.supplier_email")==F.col("email"),"left").withColumnRenamed("id","supplier_id")
         .select(
             F.col("customer_id"),
             F.col("seller_id"),
             F.col("product_id"),
             F.col("m.product_quantity"),
             F.col("date_id"),
             F.col("m.sale_quantity"),
             F.col("m.sale_total_price"),
             F.col("store_id"),
             F.col("supplier_id")
         )
)
df_facts.write.jdbc(jdbc_url, "public.facts_sales", mode="overwrite", properties=conn_props)

spark.stop()
