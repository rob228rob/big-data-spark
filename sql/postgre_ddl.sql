CREATE TABLE dim_countries
(
    id   BIGSERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE dim_cities
(
    id   BIGSERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE dim_dates
(
    id   BIGSERIAL PRIMARY KEY,
    date DATE
);

CREATE TABLE dim_pet_types
(
    id   BIGSERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE dim_pet_breeds
(
    id   BIGSERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE dim_pet_categories
(
    id   BIGSERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE dim_pets
(
    id          BIGSERIAL PRIMARY KEY,
    type_id     BIGINT REFERENCES dim_pet_types (id),
    name        VARCHAR(64),
    breed_id    BIGINT REFERENCES dim_pet_breeds (id),
    category_id BIGINT REFERENCES dim_pet_categories (id)
);

CREATE TABLE dim_customers
(
    id          BIGSERIAL PRIMARY KEY,
    first_name  VARCHAR(64),
    last_name   VARCHAR(64),
    age         BIGINT,
    email       VARCHAR(64),
    country_id  BIGINT REFERENCES dim_countries (id),
    postal_code VARCHAR(64),
    pet_id      BIGINT REFERENCES dim_pets (id)
);

CREATE TABLE dim_sellers
(
    id          BIGSERIAL PRIMARY KEY,
    first_name  VARCHAR(64),
    last_name   VARCHAR(64),
    email       VARCHAR(64),
    country_id  BIGINT REFERENCES dim_countries (id),
    postal_code VARCHAR(64)
);

CREATE TABLE dim_product_categories
(
    id   BIGSERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE dim_product_colors
(
    id   BIGSERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE dim_product_sizes
(
    id   BIGSERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE dim_product_brands
(
    id   BIGSERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE dim_product_verbose
(
    id   BIGSERIAL PRIMARY KEY,
    name VARCHAR(64)
);

CREATE TABLE dim_products
(
    id              BIGSERIAL PRIMARY KEY,
    name            VARCHAR(64),
    category_id     BIGINT REFERENCES dim_product_categories (id),
    price           DECIMAL(10, 2),
    weight          FLOAT,
    color_id        BIGINT REFERENCES dim_product_colors (id),
    size_id         BIGINT REFERENCES dim_product_sizes (id),
    brand_id        BIGINT REFERENCES dim_product_brands (id),
    material_id     BIGINT REFERENCES dim_product_verbose (id),
    description     VARCHAR(1024),
    rating          FLOAT,
    reviews         BIGINT,
    release_date_id BIGINT REFERENCES dim_dates (id),
    expiry_date_id  BIGINT REFERENCES dim_dates (id)
);

CREATE TABLE dim_stores
(
    id         BIGSERIAL PRIMARY KEY,
    name       VARCHAR(64),
    location   VARCHAR(64),
    city_id    BIGINT REFERENCES dim_cities (id),
    state      VARCHAR(64),
    country_id BIGINT REFERENCES dim_countries (id),
    phone      VARCHAR(64),
    email      VARCHAR(64)
);

CREATE TABLE dim_suppliers
(
    id         BIGSERIAL PRIMARY KEY,
    name       VARCHAR(64),
    contact    VARCHAR(64),
    email      VARCHAR(64),
    phone      VARCHAR(64),
    address    VARCHAR(64),
    city_id    BIGINT REFERENCES dim_cities (id),
    country_id BIGINT REFERENCES dim_countries (id)
);

CREATE TABLE facts_sales
(
    id               BIGSERIAL PRIMARY KEY,
    customer_id      BIGINT REFERENCES dim_customers (id),
    seller_id        BIGINT REFERENCES dim_sellers (id),
    product_id       BIGINT REFERENCES dim_products (id),
    product_quantity BIGINT,
    date_id          BIGINT REFERENCES dim_dates (id),
    quantity         BIGINT,
    total_price      DECIMAL(10, 2),
    store_id         BIGINT REFERENCES dim_stores (id),
    supplier_id      BIGINT REFERENCES dim_suppliers (id)
);
