-- Create Transformed Layer Table for Products
CREATE TABLE tl_db.products_tl (
    product_id INT,
    name STRING,
    desc_short STRING,
    brand STRING,
    category STRING,
    price DOUBLE,
    stock_status STRING,
    ean STRING,
    color STRING,
    size_code STRING,
    availability STRING,
    is_premium_product BOOLEAN
)
USING ICEBERG
TBLPROPERTIES (
  'format-version'='2',
  'write.format.default'='parquet',
  'write.merge.mode'='copy-on-write'
);
