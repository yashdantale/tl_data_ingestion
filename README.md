# 🧪 Transformed Layer – Products Table (TL)

This repo contains the transformation logic for the raw tables, transforming raw data from Iceberg into a business-ready TL table using PySpark.


Contains Below Example RAW TABLE Transformation
## 🔄 Key Features

- Reads from `raw_db.products_raw` (Iceberg)
- Applies cleaning, renaming, and logic-based transformation
- Writes to `tl_db.products_tl` (Iceberg)

## 🔧 Transformations Applied

- Title-case product names
- Truncate description to 50 chars
- Normalize `category` names (e.g., "Home & Kitchen" → "home_kitchen")
- Derive `stock_status`: Low / Medium / High
- Create `size_code`: S, M, L, XL
- Add `is_premium_product`: TRUE if price > 500

## 📁 Files

| File | Description |
|------|-------------|
| `scripts/transform_products.py` | PySpark transformation script |
| `sql/create_products_tl.sql` | DDL for TL Iceberg table |
| `config/mapping_products.json` | Raw → TL column mapping config |

> Output is written to `tl_db.products_tl` in Iceberg catalog.

## 🛠️ Requirements

```bash
pip install -r requirements.txt
# tl_data_ingestion
This repo contains the transformation logic for the raw tables, transforming raw data from Iceberg into a business-ready TL table using PySpark.
