SELECT
  schemaname as schema,
  relname AS table_name,
  pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) AS total_size_after
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(schemaname || '.' || relname) DESC
LIMIT 50;

WITH countries_cte AS (SELECT customer_country cntr
                       FROM mock_data
                       WHERE customer_country IS NOT NULL
                       UNION
                       SELECT seller_country cntr
                       FROM mock_data
                       WHERE seller_country IS NOT NULL
                       UNION
                       SELECT store_country cntr
                       FROM mock_data
                       WHERE store_country IS NOT NULL
                       UNION
                       SELECT supplier_country cntr
                       FROM mock_data
                       WHERE supplier_country IS NOT NULL)
INSERT
INTO dim_countries (name)
SELECT DISTINCT cntr
FROM countries_cte;

WITH cities_cte AS (SELECT store_city AS city
                    FROM mock_data
                    WHERE store_city IS NOT NULL
                    UNION
                    SELECT supplier_city AS city
                    FROM mock_data
                    WHERE supplier_city IS NOT NULL)
INSERT
INTO dim_cities (name)
SELECT DISTINCT city
FROM cities_cte;

WITH dates_cte AS (SELECT TO_DATE(sale_date, 'MM/DD/YYYY') d
                   FROM mock_data
                   WHERE sale_date IS NOT NULL
                   UNION
                   SELECT TO_DATE(product_release_date, 'MM/DD/YYYY') d
                   FROM mock_data
                   WHERE product_release_date IS NOT NULL
                   UNION
                   SELECT TO_DATE(product_expiry_date, 'MM/DD/YYYY') d
                   FROM mock_data
                   WHERE product_expiry_date IS NOT NULL)
INSERT
INTO dim_dates (date)
SELECT DISTINCT d
FROM dates_cte;

INSERT INTO dim_pet_types (name)
SELECT DISTINCT customer_pet_type
FROM mock_data
WHERE customer_pet_type IS NOT NULL;

INSERT INTO dim_pet_breeds (name)
SELECT DISTINCT customer_pet_breed
FROM mock_data
WHERE customer_pet_breed IS NOT NULL;

INSERT INTO dim_pet_categories (name)
SELECT DISTINCT pet_category
FROM mock_data
WHERE pet_category IS NOT NULL;

INSERT INTO dim_pets (type_id, name, breed_id, category_id)
SELECT pt.id,
       mock.customer_pet_name,
       pb.id,
       pc.id
FROM mock_data mock
         LEFT JOIN dim_pet_types pt ON mock.customer_pet_type = pt.name
         LEFT JOIN dim_pet_breeds pb ON mock.customer_pet_breed = pb.name
         LEFT JOIN dim_pet_categories pc ON mock.pet_category = pc.name
GROUP BY pt.id, mock.customer_pet_name, pb.id, pc.id;

INSERT INTO dim_product_categories (name)
SELECT DISTINCT product_category
FROM mock_data
WHERE product_category IS NOT NULL;

INSERT INTO dim_product_colors (name)
SELECT DISTINCT product_color
FROM mock_data
WHERE product_color IS NOT NULL;

INSERT INTO dim_product_sizes (name)
SELECT DISTINCT product_size
FROM mock_data
WHERE product_size IS NOT NULL;

INSERT INTO dim_product_brands (name)
SELECT DISTINCT product_brand
FROM mock_data
WHERE product_brand IS NOT NULL;

INSERT INTO dim_product_verbose (name)
SELECT DISTINCT product_material
FROM mock_data
WHERE product_material IS NOT NULL;

INSERT INTO dim_products (name, category_id, price, weight, color_id, size_id,
                          brand_id, material_id, description, rating, reviews,
                          release_date_id, expiry_date_id)
SELECT mock.product_name,
       pc.id,
       mock.product_price,
       mock.product_weight,
       clrs.id,
       "size".id,
       brands.id,
       verb.id,
       mock.product_description,
       mock.product_rating,
       mock.product_reviews,
       rd.id,
       ed.id
FROM mock_data mock
         JOIN dim_product_categories pc ON mock.product_category = pc.name
         LEFT JOIN dim_product_colors clrs ON mock.product_color = clrs.name
         LEFT JOIN dim_product_sizes "size" ON mock.product_size = "size".name
         LEFT JOIN dim_product_brands brands ON mock.product_brand = brands.name
         LEFT JOIN dim_product_verbose verb ON mock.product_material = verb.name
         LEFT JOIN dim_dates rd ON TO_DATE(mock.product_release_date, 'MM/DD/YYYY') = rd.date
         LEFT JOIN dim_dates ed ON TO_DATE(mock.product_expiry_date, 'MM/DD/YYYY') = ed.date
GROUP BY mock.product_name, pc.id, mock.product_price, mock.product_weight,
         clrs.id, "size".id, brands.id, verb.id,
         mock.product_description, mock.product_rating, mock.product_reviews,
         rd.id, ed.id;

INSERT INTO dim_customers (first_name, last_name, age, email, country_id, postal_code, pet_id)
SELECT mock.customer_first_name,
       mock.customer_last_name,
       mock.customer_age,
       mock.customer_email,
       cntry.id,
       mock.customer_postal_code,
       p.id
FROM mock_data mock
         LEFT JOIN dim_countries cntry ON mock.customer_country = cntry.name
         LEFT JOIN dim_pet_types p_types ON mock.customer_pet_type = p_types.name
         LEFT JOIN dim_pet_breeds p_breeds ON mock.customer_pet_breed = p_breeds.name
         LEFT JOIN dim_pet_categories p_cat ON mock.pet_category = p_cat.name
         LEFT JOIN dim_pets p ON mock.customer_pet_name = p.name
    AND p_types.id = p.type_id
    AND p_breeds.id = p.breed_id
    AND p_cat.id = p.category_id
GROUP BY mock.customer_first_name, mock.customer_last_name,
         mock.customer_age, mock.customer_email,
         cntry.id, mock.customer_postal_code, p.id;

INSERT INTO dim_sellers (first_name, last_name, email, country_id, postal_code)
SELECT mock.seller_first_name,
       mock.seller_last_name,
       mock.seller_email,
       cntry.id,
       mock.seller_postal_code
FROM mock_data mock
         LEFT JOIN dim_countries cntry ON mock.seller_country = cntry.name
GROUP BY mock.seller_first_name, mock.seller_last_name,
         mock.seller_email, cntry.id, mock.seller_postal_code;

INSERT INTO dim_stores (name, location, city_id, state, country_id, phone, email)
SELECT mock.store_name,
       mock.store_location,
       cities.id,
       mock.store_state,
       cntry.id,
       mock.store_phone,
       mock.store_email
FROM mock_data mock
         LEFT JOIN dim_countries cntry ON mock.store_country = cntry.name
         LEFT JOIN dim_cities cities ON mock.store_city = cities.name
GROUP BY mock.store_name, mock.store_location, cities.id,
         mock.store_state, cntry.id, mock.store_phone, mock.store_email;

INSERT INTO dim_suppliers (name, contact, email, phone, address, city_id, country_id)
SELECT mock.supplier_name,
       mock.supplier_contact,
       mock.supplier_email,
       mock.supplier_phone,
       mock.supplier_address,
       ci.id,
       co.id
FROM mock_data mock
         LEFT JOIN dim_countries co ON mock.supplier_country = co.name
         LEFT JOIN dim_cities ci ON mock.supplier_city = ci.name
GROUP BY mock.supplier_name, mock.supplier_contact,
         mock.supplier_email, mock.supplier_phone,
         mock.supplier_address, ci.id, co.id;

-- TRUNCATE facts_sales;

SELECT pg_size_pretty(pg_relation_size('public.mock_data'))     AS data_size_before;
SELECT pg_size_pretty(pg_total_relation_size('public.mock_data')) AS total_size_before;

INSERT INTO facts_sales (
    customer_id,
    seller_id,
    product_id,
    product_quantity,
    date_id,
    quantity,
    total_price,
    store_id,
    supplier_id
)
SELECT
    cu.id,
    s.id,
    products.id,
    mock.product_quantity,
    d.id,
    mock.sale_quantity,
    mock.sale_total_price,
    st.id,
    su.id
FROM mock_data AS mock
LEFT JOIN dim_customers    cu ON mock.customer_email     = cu.email
LEFT JOIN dim_sellers      s  ON mock.seller_email       = s.email
LEFT JOIN dim_product_categories p_cat ON mock.product_category = p_cat.name
LEFT JOIN dim_product_colors     p_col ON mock.product_color    = p_col.name
LEFT JOIN dim_product_sizes      p_sizes   ON mock.product_size     = p_sizes.name
LEFT JOIN dim_product_brands     p_brands   ON mock.product_brand    = p_brands.name
LEFT JOIN dim_product_verbose   p_verb   ON mock.product_material = p_verb.name
LEFT JOIN dim_dates              dates   ON TO_DATE(mock.product_release_date, 'MM/DD/YYYY') = dates.date
LEFT JOIN dim_dates              date_e   ON TO_DATE(mock.product_expiry_date,  'MM/DD/YYYY') = date_e.date
LEFT JOIN dim_products           products    ON mock.product_name       = products.name
    AND p_cat.id   = products.category_id
    AND mock.product_price  = products.price
    AND mock.product_weight = products.weight
    AND p_col.id  = products.color_id
    AND p_sizes.id    = products.size_id
    AND p_brands.id    = products.brand_id
    AND p_verb.id    = products.material_id
    AND mock.product_description = products.description
    AND mock.product_rating      = products.rating
    AND mock.product_reviews     = products.reviews
    AND dates.id    = products.release_date_id
    AND date_e.id    = products.expiry_date_id
LEFT JOIN dim_dates    d  ON TO_DATE(mock.sale_date, 'MM/DD/YYYY') = d.date
LEFT JOIN dim_stores   st ON mock.store_email       = st.email
LEFT JOIN dim_suppliers su ON mock.supplier_email    = su.email;

SELECT pg_size_pretty(pg_relation_size('public.mock_data'))     AS data_size_after;
SELECT pg_size_pretty(pg_total_relation_size('public.mock_data')) AS total_size_after;

SELECT
  schemaname as schema,
  relname AS table_name,
  pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) AS total_size_after
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(schemaname || '.' || relname) DESC
LIMIT 50;