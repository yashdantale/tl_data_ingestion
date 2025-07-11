import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, round, when, substring, initcap

# Load column mapping
with open("config/mapping_products.json") as f:
    mapping = json.load(f)

# Create Spark session with Iceberg support
spark = SparkSession.builder \
    .appName("TransformProductsTL") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# ✅ Read from Iceberg raw table
df_raw = spark.read.format("iceberg").load("raw_db.products")

# Rename columns using mapping
rename_exprs = [col(c["source"]).alias(c["target"]) for c in mapping["columns"]]
df = df_raw.select(rename_exprs)

# Apply transformations
df = df.withColumn("name", initcap(col("name"))) \
       .withColumn("desc_short", substring(col("desc_short"), 1, 50)) \
       .withColumn("category", regexp_replace(lower(col("category")), "[^a-z0-9]+", "_")) \
       .withColumn("price", round(col("price"), 2)) \
       .withColumn("stock_status", when(col("stock") < 200, "Low")
                   .when(col("stock") < 500, "Medium")
                   .otherwise("High")) \
       .withColumn("availability", initcap(col("availability"))) \
       .withColumn("size_code", when(col("size") == "Small", "S")
                   .when(col("size") == "Medium", "M")
                   .when(col("size") == "Large", "L")
                   .when(col("size") == "Extra Large", "XL")
                   .otherwise(col("size"))) \
       .withColumn("is_premium_product", col("price") > 500)

df = df.drop("stock", "size")

# ✅ Write to TL Iceberg table
df.writeTo("tl_db.products_tl").using("iceberg").createOrReplace()

print("✅ Transformed data written to 'tl_db.products_tl'")
