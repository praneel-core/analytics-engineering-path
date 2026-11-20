# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · E-commerce SQL Practice Lab — Setup
# MAGIC
# MAGIC Builds a complete, realistic e-commerce dataset (14 interconnected Delta tables) for the
# MAGIC **250-question SQL practice lab**.
# MAGIC
# MAGIC **How to run (Databricks Free Edition)**
# MAGIC 1. Import this file into your workspace (*Workspace → Import*). Because of the first line
# MAGIC    (`# Databricks notebook source`) Databricks opens it as a notebook.
# MAGIC 2. Attach to **Serverless** compute and click **Run all**. Runtime is a few minutes.
# MAGIC 3. The script is **safe to re-run**: it only drops and rebuilds the lab's own tables inside the
# MAGIC    `ecommerce_lab` schema. Nothing else in your workspace is touched.
# MAGIC 4. The data is **deterministic** — every run produces exactly the same rows.
# MAGIC
# MAGIC **What gets created** (catalog `workspace`, schema `ecommerce_lab`)
# MAGIC
# MAGIC | Group | Tables |
# MAGIC |---|---|
# MAGIC | Core (A–J) | `customers`, `orders`, `order_items`, `products`, `categories`, `payments`, `returns`, `inventory`, `warehouses`, `customer_activity` |
# MAGIC | Extras | `order_status_history`, `customer_changes`, `dim_date`, `orders_incoming_batch` |
# MAGIC
# MAGIC **Data snapshot date: `2025-12-31`.** Treat it as "today" for every question (never use `current_date()`).
# MAGIC Order data spans `2022-01-01` → `2025-12-31`.
# MAGIC
# MAGIC **Design notes**
# MAGIC * Dimension tables are generated in Python with a seeded `random.Random`; fact tables are generated
# MAGIC   with set-based Spark SQL using `xxhash64()` of row keys as a deterministic pseudo-random source.
# MAGIC * Only the DataFrame/SQL APIs are used (no RDDs, no `sparkContext`) so it runs on Serverless compute.
# MAGIC * Primary/foreign keys are *informational* in Databricks (declared, not enforced).
# MAGIC * The data deliberately contains realistic quality problems. They are documented separately in
# MAGIC   `DATA_QUALITY_ISSUES.md` — **read that only after you have tried Level 8** if you want to discover
# MAGIC   them yourself.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Configuration and helpers

# COMMAND ----------

import os
import random
import datetime as dt

# --- Where to build the lab -------------------------------------------------------------------------
# Databricks Free Edition (Unity Catalog) exposes the default catalog `workspace`.
# Set CATALOG = "" to skip `USE CATALOG` (useful for a local PySpark test run).
CATALOG = os.environ.get("LAB_CATALOG", "workspace")
SCHEMA = os.environ.get("LAB_SCHEMA", "ecommerce_lab")
# DELTA on Databricks. (A local plain-Spark test run can set LAB_FORMAT=parquet.)
FMT = os.environ.get("LAB_FORMAT", "DELTA")

# --- Data volume knobs (the defaults satisfy the lab's minimum sizes) -------------------------------
N_CUSTOMERS_BASE = 11820   # distinct people
N_CUSTOMER_DUPES = 180     # people who re-registered (duplicate customer records) -> 12,000 rows
N_PRODUCTS = 1200
N_ORDERS_REGULAR = 60000
N_SESSIONS = 30000
SNAPSHOT = "2025-12-31"
SEED = 20250101

# --- Spark session (Databricks provides `spark`; create one only when running locally) ---------------
try:
    spark  # noqa: F821
except NameError:
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[4]").appName("ecommerce_lab_local_test")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.driver.memory", "6g")
             .getOrCreate())

try:  # keep timestamps identical on every cluster
    spark.conf.set("spark.sql.session.timeZone", "UTC")
except Exception as e:  # pragma: no cover
    print("Could not set session time zone (continuing):", e)


def run(sql_text):
    """Run one SQL statement."""
    return spark.sql(sql_text)


def show(df, n=50):
    """display() in Databricks, .show() elsewhere."""
    try:
        display(df)  # noqa: F821
    except NameError:
        df.show(n, truncate=False)


def U(key, salt):
    """Deterministic pseudo-random number in [0, 1) derived from a row key + a salt string."""
    return f"(pmod(xxhash64({key}, '{salt}'), 1000000) / 1000000.0)"


def H(key, salt, mod):
    """Deterministic pseudo-random integer in [0, mod) derived from a row key + a salt string."""
    return f"CAST(pmod(xxhash64({key}, '{salt}'), {mod}) AS INT)"


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Create the schema and drop any previous lab tables (makes the script re-runnable)

# COMMAND ----------

if CATALOG:
    try:
        run(f"USE CATALOG {CATALOG}")
    except Exception as e:  # e.g. a workspace without Unity Catalog: carry on in the default catalog
        print(f"WARNING: could not USE CATALOG {CATALOG} ({type(e).__name__}); continuing in the current default catalog.")
        CATALOG = ""
run(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA} COMMENT 'Practice schema for the 250-question SQL lab (synthetic e-commerce data)'")
run(f"USE SCHEMA {SCHEMA}")

LAB_TABLES = [
    "customers", "orders", "order_items", "products", "categories", "payments", "returns",
    "inventory", "warehouses", "customer_activity", "order_status_history", "customer_changes",
    "dim_date", "orders_incoming_batch",
]
HELPER_TABLES = [
    "_gen_cities", "_gen_customers", "_gen_buyers", "_gen_tickets", "_gen_orders_a", "_gen_orders",
    "_gen_items_base", "_gen_order_totals", "_gen_pay", "_gen_sessions",
]
for t in LAB_TABLES + HELPER_TABLES:
    run(f"DROP TABLE IF EXISTS {t}")
print("Schema ready:", (CATALOG + "." if CATALOG else "") + SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Table definitions (types, NOT NULL keys, table + column comments)
# MAGIC
# MAGIC Keys are documented here and declared as *informational* constraints in section 14.
# MAGIC `PK` = primary key, `FK` = foreign key (see the comments).

# COMMAND ----------

TABLES = {
    "categories": ("Product category hierarchy (up to 3 levels). PK category_id; FK parent_category_id -> categories.category_id (NULL for root categories).", [
        ("category_id", "INT NOT NULL", "PK. Category identifier"),
        ("category_name", "STRING", "Category name"),
        ("parent_category_id", "INT", "FK to categories.category_id; NULL = top-level category"),
    ]),
    "warehouses": ("Fulfilment centres that hold inventory. PK warehouse_id.", [
        ("warehouse_id", "INT NOT NULL", "PK. Warehouse identifier"),
        ("warehouse_name", "STRING", "Warehouse name"),
        ("city", "STRING", "Warehouse city"),
        ("country", "STRING", "Warehouse country"),
    ]),
    "products": ("Product catalogue (current attributes). PK product_id; FK category_id -> categories. Contains discontinued products.", [
        ("product_id", "INT NOT NULL", "PK. Product identifier"),
        ("product_name", "STRING", "Product display name"),
        ("category_id", "INT", "FK to categories.category_id (leaf category)"),
        ("brand", "STRING", "Brand name (may be NULL)"),
        ("unit_cost", "DECIMAL(12,2)", "Cost of goods per unit (INR)"),
        ("selling_price", "DECIMAL(12,2)", "Current list price per unit (INR)"),
        ("launch_date", "DATE", "Date the product was first listed"),
        ("is_active", "BOOLEAN", "FALSE = discontinued / delisted"),
        ("discontinued_date", "DATE", "Date the product was delisted (NULL if never)"),
    ]),
    "customers": ("Customer master. PK customer_id. Some people appear twice (re-registered accounts).", [
        ("customer_id", "INT NOT NULL", "PK. Customer identifier"),
        ("first_name", "STRING", "First name"),
        ("last_name", "STRING", "Last name"),
        ("email", "STRING", "Email address (may be missing/invalid)"),
        ("gender", "STRING", "Gender as captured at sign-up"),
        ("date_of_birth", "DATE", "Date of birth"),
        ("city", "STRING", "City of residence"),
        ("state", "STRING", "State / region"),
        ("country", "STRING", "Country"),
        ("signup_date", "DATE", "Account creation date"),
        ("customer_segment", "STRING", "Loyalty tier: Bronze / Silver / Gold / Platinum"),
        ("is_active", "BOOLEAN", "CRM active flag (not always reliable)"),
        ("updated_at", "TIMESTAMP", "Last time the CRM record changed"),
    ]),
    "orders": ("Order headers, one row per order. PK order_id; FK customer_id -> customers. total_amount = SUM(order_items.line_total) + tax_amount + shipping_amount.", [
        ("order_id", "BIGINT NOT NULL", "PK. Order identifier"),
        ("customer_id", "INT", "FK to customers.customer_id"),
        ("order_date", "TIMESTAMP", "When the order was placed"),
        ("order_status", "STRING", "Pending / Processing / Shipped / Delivered / Cancelled / Returned / Failed"),
        ("shipping_address", "STRING", "Free-text delivery address"),
        ("payment_method", "STRING", "Payment method chosen at checkout"),
        ("total_amount", "DECIMAL(12,2)", "Amount payable (INR) = items after discount + tax + shipping"),
        ("discount_amount", "DECIMAL(12,2)", "Total discount already applied inside the line totals"),
        ("tax_amount", "DECIMAL(12,2)", "GST (18%) on discounted item value"),
        ("shipping_amount", "DECIMAL(12,2)", "Shipping fee (free above INR 999)"),
        ("updated_at", "TIMESTAMP", "Last status change"),
        ("loaded_at", "TIMESTAMP", "When the record landed in the warehouse (can be late)"),
    ]),
    "order_items": ("Order lines. PK order_item_id; FK order_id -> orders; FK product_id -> products. line_total = quantity*unit_price - discount.", [
        ("order_item_id", "BIGINT NOT NULL", "PK. Order line identifier"),
        ("order_id", "BIGINT", "FK to orders.order_id"),
        ("product_id", "INT", "FK to products.product_id"),
        ("quantity", "INT", "Units purchased"),
        ("unit_price", "DECIMAL(12,2)", "Price per unit at purchase time (INR)"),
        ("discount", "DECIMAL(12,2)", "Discount amount on the whole line (INR, not %)"),
        ("line_total", "DECIMAL(12,2)", "quantity * unit_price - discount"),
    ]),
    "payments": ("Payment attempts/transactions. PK payment_id; FK order_id -> orders. An order can have several attempts.", [
        ("payment_id", "BIGINT NOT NULL", "PK. Payment identifier"),
        ("order_id", "BIGINT", "FK to orders.order_id"),
        ("payment_date", "TIMESTAMP", "When the attempt was made"),
        ("payment_method", "STRING", "Method used for this attempt"),
        ("payment_status", "STRING", "Success / Failed / Pending / Refunded"),
        ("payment_amount", "DECIMAL(12,2)", "Amount of this attempt (INR)"),
        ("transaction_reference", "STRING", "Gateway reference (should be unique per transaction)"),
    ]),
    "returns": ("Customer returns at order-line level. PK return_id; FK order_item_id -> order_items.", [
        ("return_id", "BIGINT NOT NULL", "PK. Return identifier"),
        ("order_item_id", "BIGINT", "FK to order_items.order_item_id"),
        ("return_date", "TIMESTAMP", "When the return was raised"),
        ("return_reason", "STRING", "Reason given by the customer"),
        ("returned_quantity", "INT", "Units returned"),
        ("refund_amount", "DECIMAL(12,2)", "Refund value (INR)"),
        ("return_status", "STRING", "Pending / Approved / Rejected / Refunded"),
    ]),
    "inventory": ("Stock per product per warehouse (current snapshot). PK inventory_id; FK product_id -> products; FK warehouse_id -> warehouses.", [
        ("inventory_id", "INT NOT NULL", "PK. Inventory row identifier"),
        ("product_id", "INT", "FK to products.product_id"),
        ("warehouse_id", "INT", "FK to warehouses.warehouse_id"),
        ("stock_quantity", "INT", "Units physically on hand"),
        ("reserved_quantity", "INT", "Units already allocated to open orders"),
        ("reorder_level", "INT", "Re-order trigger level"),
        ("last_updated_at", "TIMESTAMP", "Last stock movement"),
    ]),
    "customer_activity": ("Clickstream-style events. PK activity_id; FK customer_id -> customers (NULL for anonymous sessions); FK product_id -> products.", [
        ("activity_id", "BIGINT NOT NULL", "PK. Event identifier"),
        ("customer_id", "INT", "FK to customers.customer_id; NULL = anonymous visitor"),
        ("activity_timestamp", "TIMESTAMP", "Event time"),
        ("activity_type", "STRING", "login / page_view / search / product_view / wishlist_add / add_to_cart / remove_from_cart / checkout_start / purchase"),
        ("session_id", "STRING", "Browsing session identifier"),
        ("device_type", "STRING", "Device used"),
        ("product_id", "INT", "FK to products.product_id for product-related events"),
    ]),
    "order_status_history": ("Status transitions per order (event log). PK history_id; FK order_id -> orders.", [
        ("history_id", "BIGINT NOT NULL", "PK. History row identifier"),
        ("order_id", "BIGINT", "FK to orders.order_id"),
        ("order_status", "STRING", "Status the order moved into"),
        ("status_timestamp", "TIMESTAMP", "When the order entered that status"),
        ("changed_by", "STRING", "System or team that made the change"),
    ]),
    "customer_changes": ("Change-data-capture feed of customer attribute changes (use it for SCD exercises). PK change_id; FK customer_id -> customers.", [
        ("change_id", "BIGINT NOT NULL", "PK. Feed record identifier"),
        ("customer_id", "INT", "FK to customers.customer_id"),
        ("change_timestamp", "TIMESTAMP", "When the change happened in the source system"),
        ("received_at", "TIMESTAMP", "When the record arrived in the warehouse"),
        ("change_type", "STRING", "INSERT (first version) or UPDATE"),
        ("city", "STRING", "City after the change"),
        ("state", "STRING", "State after the change"),
        ("customer_segment", "STRING", "Segment after the change"),
        ("is_active", "BOOLEAN", "Active flag after the change"),
    ]),
    "dim_date": ("Calendar dimension (2021-12-01 to 2026-12-31) with Indian fiscal year (Apr-Mar) and holidays. PK date_key.", [
        ("date_key", "INT NOT NULL", "PK. yyyymmdd"),
        ("full_date", "DATE", "Calendar date"),
        ("year", "INT", "Calendar year"),
        ("quarter", "INT", "Calendar quarter 1-4"),
        ("month", "INT", "Month number 1-12"),
        ("month_name", "STRING", "Month name"),
        ("week_of_year", "INT", "ISO-style week number"),
        ("day_of_week", "INT", "1 = Monday ... 7 = Sunday"),
        ("day_name", "STRING", "Day name"),
        ("is_weekend", "BOOLEAN", "Saturday or Sunday"),
        ("is_holiday", "BOOLEAN", "Public holiday / major festival"),
        ("holiday_name", "STRING", "Holiday name"),
        ("fiscal_year", "INT", "Fiscal year label = calendar year in which the FY ends (FY runs Apr-Mar)"),
        ("fiscal_quarter", "INT", "Fiscal quarter 1-4 (Q1 = Apr-Jun)"),
    ]),
    "orders_incoming_batch": ("Next incremental load for `orders` (arrives after the snapshot): new orders, changed orders, late arrivals and re-sent rows. Same columns as orders + batch_id.", [
        ("order_id", "BIGINT", "Order identifier (NOT unique in this batch)"),
        ("customer_id", "INT", "Customer identifier (some customers may not exist yet)"),
        ("order_date", "TIMESTAMP", "When the order was placed"),
        ("order_status", "STRING", "Order status in this batch"),
        ("shipping_address", "STRING", "Delivery address"),
        ("payment_method", "STRING", "Payment method"),
        ("total_amount", "DECIMAL(12,2)", "Amount payable (INR)"),
        ("discount_amount", "DECIMAL(12,2)", "Discount applied (INR)"),
        ("tax_amount", "DECIMAL(12,2)", "Tax (INR)"),
        ("shipping_amount", "DECIMAL(12,2)", "Shipping fee (INR)"),
        ("updated_at", "TIMESTAMP", "Last change time in the source"),
        ("loaded_at", "TIMESTAMP", "When the row landed in this batch"),
        ("batch_id", "STRING", "Load batch identifier"),
    ]),
}


def create_table(name):
    comment, cols = TABLES[name]
    col_sql = ",\n  ".join(f"{c} {t} COMMENT '{d}'" for c, t, d in cols)
    run(f"CREATE TABLE {name} (\n  {col_sql}\n) USING {FMT} COMMENT '{comment}'")


for _t in TABLES:
    create_table(_t)
print(f"Created {len(TABLES)} empty tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Dimension data generated in Python (seeded): categories, warehouses, products

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField, IntegerType, LongType, StringType, DateType,
                               TimestampType, BooleanType)

rng = random.Random(SEED)
D = dt.date
SNAP = D(2025, 12, 31)

# ---- Categories: 8 roots -> level-2 -> a few level-3 (hierarchy for self-join / recursive exercises) ----
CATEGORIES = [
    (1, "Electronics", None), (2, "Fashion", None), (3, "Home & Kitchen", None),
    (4, "Beauty & Personal Care", None), (5, "Sports & Fitness", None), (6, "Books & Stationery", None),
    (7, "Toys & Baby", None), (8, "Grocery & Gourmet", None),
    (9, "Mobiles & Accessories", 1), (10, "Laptops & Computers", 1), (11, "Audio", 1), (12, "Cameras", 1),
    (13, "Wearables", 1),
    (14, "Men's Clothing", 2), (15, "Women's Clothing", 2), (16, "Footwear", 2), (17, "Watches & Accessories", 2),
    (18, "Cookware", 3), (19, "Furniture", 3), (20, "Home Decor", 3), (21, "Appliances", 3),
    (22, "Skincare", 4), (23, "Haircare", 4), (24, "Makeup", 4), (25, "Fragrances", 4),
    (26, "Gym Equipment", 5), (27, "Outdoor & Camping", 5), (28, "Cycling", 5), (29, "Yoga & Fitness Accessories", 5),
    (30, "Fiction", 6), (31, "Non-fiction", 6), (32, "Stationery", 6), (33, "Academic & Exam Prep", 6),
    (34, "Toys & Games", 7), (35, "Baby Care", 7), (36, "Kids' Clothing", 7),
    (37, "Snacks", 8), (38, "Beverages", 8), (39, "Staples", 8), (40, "Health Foods", 8),
    (41, "Smartphones", 9), (42, "Cases & Covers", 9), (43, "Chargers & Cables", 9), (44, "Power Banks", 9),
    (45, "Laptops", 10), (46, "Tablets", 10), (47, "Computer Accessories", 10),
]

# leaf category -> (price_lo, price_hi, sampling_weight, brands, product nouns)
LEAVES = {
    41: (6999, 94999, 3, ["Samsung", "Apple", "Xiaomi", "OnePlus", "Realme", "Motorola", "Nothing"], ["5G Smartphone", "Smartphone 128GB", "Smartphone 256GB"]),
    42: (149, 1999, 2, ["Spigen", "Ringke", "Nillkin", "Urban Armor", "Caseology"], ["Back Case", "Flip Cover", "Tempered Glass"]),
    43: (249, 2999, 2, ["Anker", "Belkin", "Ambrane", "Portronics", "Mi"], ["Fast Charger", "USB-C Cable", "Wireless Charger"]),
    44: (799, 4999, 2, ["Anker", "Mi", "Ambrane", "Realme", "Syska"], ["10000mAh Power Bank", "20000mAh Power Bank"]),
    45: (24999, 179999, 2, ["Dell", "HP", "Lenovo", "Asus", "Acer", "Apple"], ["Laptop 14", "Laptop 15", "Gaming Laptop"]),
    46: (8999, 89999, 1.5, ["Apple", "Samsung", "Lenovo", "Xiaomi"], ["Tablet 10", "Tablet Pro"]),
    47: (299, 7999, 2, ["Logitech", "Dell", "HP", "Zebronics", "Redragon"], ["Wireless Mouse", "Keyboard", "Webcam", "USB Hub"]),
    11: (499, 29999, 3, ["Sony", "JBL", "Boat", "Bose", "Sennheiser", "Noise"], ["Wireless Earbuds", "Bluetooth Speaker", "Headphones"]),
    12: (3999, 119999, 1, ["Canon", "Nikon", "Sony", "GoPro", "Fujifilm"], ["Mirrorless Camera", "Action Camera", "Lens"]),
    13: (1299, 44999, 2, ["Apple", "Samsung", "Noise", "Fitbit", "Garmin", "Amazfit"], ["Smartwatch", "Fitness Band"]),
    14: (399, 4999, 3, ["Levis", "Allen Solly", "Peter England", "H&M", "Raymond", "Puma"], ["Cotton T-Shirt", "Slim Fit Jeans", "Formal Shirt"]),
    15: (499, 6999, 3, ["Biba", "W", "Zara", "H&M", "Libas", "Vero Moda"], ["Kurta Set", "Summer Dress", "Palazzo Pants"]),
    16: (599, 9999, 3, ["Nike", "Adidas", "Puma", "Bata", "Skechers", "Woodland"], ["Running Shoes", "Sneakers", "Sandals"]),
    17: (499, 19999, 2, ["Titan", "Fossil", "Casio", "Fastrack", "Timex"], ["Analog Watch", "Leather Wallet", "Sunglasses"]),
    18: (399, 6999, 1.5, ["Prestige", "Hawkins", "Pigeon", "Tefal", "Milton"], ["Non-stick Pan", "Pressure Cooker", "Cookware Set"]),
    19: (1999, 49999, 1.5, ["Wakefit", "Godrej Interio", "Nilkamal", "IKEA", "Durian"], ["Office Chair", "Study Table", "Bookshelf"]),
    20: (199, 9999, 1.5, ["Home Centre", "Wallmantra", "Chumbak", "Fabindia", "IKEA"], ["Wall Art", "Table Lamp", "Cushion Cover Set"]),
    21: (1499, 59999, 2, ["Philips", "Bajaj", "LG", "Havells", "Samsung", "IFB"], ["Mixer Grinder", "Air Fryer", "Microwave Oven"]),
    22: (149, 3999, 2, ["Cetaphil", "Neutrogena", "Minimalist", "Lakme", "Nivea"], ["Moisturiser", "Sunscreen SPF50", "Face Serum"]),
    23: (149, 2999, 2, ["L'Oreal", "Dove", "Head & Shoulders", "Mamaearth", "Pantene"], ["Shampoo", "Hair Oil", "Conditioner"]),
    24: (199, 4999, 1.5, ["Maybelline", "Lakme", "MAC", "Nykaa", "Revlon"], ["Lipstick", "Foundation", "Kajal"]),
    25: (299, 9999, 1.5, ["Davidoff", "Wild Stone", "Engage", "Fogg", "Calvin Klein"], ["Eau de Parfum", "Body Spray", "Deodorant"]),
    26: (499, 29999, 1.5, ["Decathlon", "Cosco", "Fitkit", "PowerMax", "Kore"], ["Dumbbell Set", "Treadmill", "Resistance Bands"]),
    27: (699, 14999, 1, ["Quechua", "Wildcraft", "Coleman", "Skybags", "Trekker"], ["Camping Tent", "Backpack 40L", "Sleeping Bag"]),
    28: (1999, 59999, 1, ["Hero", "Firefox", "Btwin", "Giant", "Hercules"], ["Mountain Bike", "Cycling Helmet", "Bike Lock"]),
    29: (199, 3999, 1.5, ["Reebok", "Boldfit", "Strauss", "Adidas", "Decathlon"], ["Yoga Mat", "Foam Roller", "Skipping Rope"]),
    30: (149, 899, 1.5, ["Penguin", "HarperCollins", "Rupa", "Bloomsbury"], ["Novel", "Thriller Paperback", "Fantasy Series Set"]),
    31: (199, 1499, 1.5, ["Penguin", "Jaico", "Oxford", "Pan Macmillan"], ["Self-help Book", "Biography", "Business Book"]),
    32: (49, 1999, 1.5, ["Parker", "Classmate", "Camlin", "Pilot", "Faber-Castell"], ["Notebook Pack", "Fountain Pen", "Marker Set"]),
    33: (199, 2499, 1.5, ["Arihant", "Oswaal", "Pearson", "McGraw Hill", "Disha"], ["JEE Guide", "UPSC Prelims Guide", "Textbook"]),
    34: (199, 7999, 2, ["Lego", "Funskool", "Hot Wheels", "Hasbro", "Mattel"], ["Building Blocks Set", "Board Game", "Remote Control Car"]),
    35: (149, 5999, 2, ["Pampers", "Huggies", "Johnson's", "Chicco", "Himalaya"], ["Diaper Pack", "Baby Lotion", "Stroller"]),
    36: (299, 2999, 1.5, ["Gini & Jony", "H&M", "Allen Solly Junior", "Max", "FirstCry"], ["Kids T-Shirt", "Frock", "Pyjama Set"]),
    37: (20, 699, 2, ["Haldiram's", "Lay's", "Britannia", "Bikano", "Kurkure"], ["Namkeen Pack", "Chips Combo", "Biscuit Box"]),
    38: (49, 1999, 2, ["Tata Tea", "Nescafe", "Bru", "Red Bull", "Paper Boat"], ["Tea Pack", "Instant Coffee", "Juice Pack"]),
    39: (99, 1999, 2, ["Aashirvaad", "Fortune", "Tata Sampann", "India Gate", "Daawat"], ["Atta 5kg", "Basmati Rice 5kg", "Cooking Oil 5L"]),
    40: (199, 3999, 1.5, ["Quaker", "Kellogg's", "Saffola", "MuscleBlaze", "Yoga Bar"], ["Oats Pack", "Protein Powder", "Muesli"]),
}
SERIES = ["Alpha", "Neo", "Prime", "Lite", "Max", "Plus", "Air", "Edge", "Core", "Vista", "Nova", "Zen"]

WAREHOUSES = [
    (1, "Bhiwandi Fulfilment Centre", "Mumbai", "India"), (2, "Gurugram Mega Hub", "Delhi", "India"),
    (3, "Hosur Road Hub", "Bengaluru", "India"), (4, "Sriperumbudur Hub", "Chennai", "India"),
    (5, "Shamshabad Hub", "Hyderabad", "India"), (6, "Dankuni Hub", "Kolkata", "India"),
    (7, "Chakan Hub", "Pune", "India"), (8, "Sanand Hub", "Ahmedabad", "India"),
    (9, "Sitapura Hub", "Jaipur", "India"), (10, "Jebel Ali Hub", "Dubai", "United Arab Emirates"),
    (11, "Tuas Hub", "Singapore", "Singapore"), (12, "Park Royal Hub", "London", "United Kingdom"),
]


def price_round(p):
    if p >= 1000:
        return round(p / 100) * 100 - 1
    if p >= 100:
        return round(p / 10) * 10 - 1
    return max(round(p), 5)


leaf_ids = list(LEAVES.keys())
leaf_w = [LEAVES[i][2] for i in leaf_ids]
PRODUCTS = []
for pid in range(1, N_PRODUCTS + 1):
    cat = rng.choices(leaf_ids, weights=leaf_w)[0]
    lo, hi, _, brands, nouns = LEAVES[cat]
    brand = rng.choice(brands)
    # log-uniform price inside the category band
    price = price_round(lo * (hi / lo) ** rng.random())
    cost = round(price * rng.uniform(0.45, 0.85), 2)
    name = f"{brand} {rng.choice(nouns)} {rng.choice(SERIES)} {rng.randint(1, 99)}"
    if pid <= 300:   # evergreen catalogue: launched before the first customer, never discontinued
        launch = D(2020, 1, 1) + dt.timedelta(days=rng.randint(0, 730))
        disc_date, active = None, True
    else:
        launch = D(2021, 6, 1) + dt.timedelta(days=rng.randint(0, 1582))   # up to 2025-09-30
        disc_date, active = None, True
        if rng.random() < 0.08:
            cand = launch + dt.timedelta(days=rng.randint(120, 900))
            if cand <= D(2025, 11, 30):
                disc_date, active = cand, False
    PRODUCTS.append([pid, name, cat, brand, float(cost), float(price), launch, active, disc_date])

# --- deliberate product quality issues (documented in DATA_QUALITY_ISSUES.md) ---
for p in PRODUCTS:
    r = rng.random()
    if r < 0.02:
        p[1] = "  " + p[1] + " "           # stray spaces
    elif r < 0.035:
        p[1] = p[1].upper()                # inconsistent casing
    r = rng.random()
    if r < 0.03:
        p[3] = None                        # missing brand
    elif r < 0.05:
        p[3] = p[3].lower()
    elif r < 0.065:
        p[3] = p[3].upper()
for pid in rng.sample(range(301, N_PRODUCTS + 1), 10):   # negative-margin products (cost > price)
    PRODUCTS[pid - 1][4] = round(PRODUCTS[pid - 1][5] * 1.12, 2)
for pid in rng.sample(range(301, N_PRODUCTS + 1), 5):    # inactive but no discontinued_date
    if PRODUCTS[pid - 1][8] is None:
        PRODUCTS[pid - 1][7] = False
for pid in rng.sample(range(301, N_PRODUCTS + 1), 3):    # active but has a discontinued_date
    if PRODUCTS[pid - 1][8] is None:
        PRODUCTS[pid - 1][8] = D(2025, 3, 1)

schema_prod = StructType([
    StructField("product_id", IntegerType()), StructField("product_name", StringType()),
    StructField("category_id", IntegerType()), StructField("brand", StringType()),
    StructField("unit_cost", StringType()), StructField("selling_price", StringType()),
    StructField("launch_date", DateType()), StructField("is_active", BooleanType()),
    StructField("discontinued_date", DateType()),
])
rows = [(p[0], p[1], p[2], p[3], str(p[4]), str(p[5]), p[6], p[7], p[8]) for p in PRODUCTS]
spark.createDataFrame(rows, schema_prod).createOrReplaceTempView("v_products")
spark.createDataFrame(CATEGORIES, StructType([StructField("category_id", IntegerType()), StructField("category_name", StringType()),
                                              StructField("parent_category_id", IntegerType())])).createOrReplaceTempView("v_categories")
spark.createDataFrame(WAREHOUSES, StructType([StructField("warehouse_id", IntegerType()), StructField("warehouse_name", StringType()),
                                              StructField("city", StringType()), StructField("country", StringType())])).createOrReplaceTempView("v_warehouses")

run("INSERT INTO categories SELECT category_id, category_name, parent_category_id FROM v_categories")
run("INSERT INTO warehouses SELECT warehouse_id, warehouse_name, city, country FROM v_warehouses")
run("""INSERT INTO products
       SELECT product_id, product_name, category_id, brand, CAST(unit_cost AS DECIMAL(12,2)),
              CAST(selling_price AS DECIMAL(12,2)), launch_date, is_active, discontinued_date
       FROM v_products""")
print("categories:", spark.table("categories").count(), "| warehouses:", spark.table("warehouses").count(),
      "| products:", spark.table("products").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Customers (seeded Python) — includes duplicate re-registrations and messy text

# COMMAND ----------

CITIES = [  # idx, city, state, country, weight, postal prefix
    (1, "Mumbai", "Maharashtra", "India", 11, "400"), (2, "Delhi", "Delhi", "India", 10, "110"),
    (3, "Bengaluru", "Karnataka", "India", 10, "560"), (4, "Hyderabad", "Telangana", "India", 7, "500"),
    (5, "Chennai", "Tamil Nadu", "India", 6, "600"), (6, "Kolkata", "West Bengal", "India", 5, "700"),
    (7, "Pune", "Maharashtra", "India", 6, "411"), (8, "Ahmedabad", "Gujarat", "India", 4, "380"),
    (9, "Jaipur", "Rajasthan", "India", 3, "302"), (10, "Lucknow", "Uttar Pradesh", "India", 2.5, "226"),
    (11, "Kochi", "Kerala", "India", 2.5, "682"), (12, "Chandigarh", "Chandigarh", "India", 2, "160"),
    (13, "Indore", "Madhya Pradesh", "India", 2, "452"), (14, "Surat", "Gujarat", "India", 2.5, "395"),
    (15, "Gurugram", "Haryana", "India", 4, "122"), (16, "Noida", "Uttar Pradesh", "India", 3.5, "201"),
    (17, "Dubai", "Dubai", "United Arab Emirates", 3, "000"), (18, "Singapore", "Singapore", "Singapore", 2, "018"),
    (19, "London", "England", "United Kingdom", 2.5, "EC1"), (20, "New York", "New York", "United States", 3, "100"),
    (21, "Sydney", "New South Wales", "Australia", 1, "200"),
]
FIRST_F = ["Aadhya", "Ananya", "Diya", "Isha", "Kavya", "Meera", "Neha", "Priya", "Riya", "Saanvi", "Sneha", "Tanvi",
           "Pooja", "Nisha", "Shreya", "Ritu", "Anjali", "Divya", "Kritika", "Lakshmi", "Emily", "Olivia", "Sophia",
           "Fatima", "Aisha", "Chloe", "Hannah", "Mei", "Sarah", "Zara"]
FIRST_M = ["Aarav", "Arjun", "Rohan", "Vivaan", "Kabir", "Aditya", "Rahul", "Amit", "Karan", "Siddharth", "Vikram",
           "Nikhil", "Manish", "Rajesh", "Sanjay", "Deepak", "Varun", "Harsh", "Ishaan", "Yash", "James", "Liam",
           "Noah", "Omar", "Ethan", "Daniel", "Ryan", "Wei", "Ahmed", "Lucas"]
LAST = ["Sharma", "Verma", "Gupta", "Singh", "Patel", "Reddy", "Iyer", "Nair", "Mehta", "Joshi", "Kapoor", "Malhotra",
        "Chatterjee", "Banerjee", "Das", "Rao", "Kulkarni", "Desai", "Shah", "Agarwal", "Bose", "Menon", "Pillai",
        "Khan", "Ali", "Thomas", "Fernandes", "Dsouza", "Sinha", "Mishra", "Chopra", "Bhatia", "Saxena", "Jain",
        "Pandey", "Smith", "Johnson", "Brown", "Taylor", "Wilson", "Lim", "Tan", "Chen"]
DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "proton.me", "example.com"]
SEGMENTS = ["Bronze", "Silver", "Gold", "Platinum"]
SEG_W = [45, 30, 18, 7]
ALT_CITY = {"Bengaluru": ("Bangalore", 0.04), "Mumbai": ("Bombay", 0.03), "Gurugram": ("Gurgaon", 0.04),
            "Delhi": ("New Delhi", 0.06), "Kolkata": ("Calcutta", 0.03), "Chennai": ("Madras", 0.02)}
END = D(2025, 12, 31)


def pad(s):
    return rng.choice(["  ", " ", ""]) + s + rng.choice(["  ", " "])


gen_rows, cust_rows = [], []
city_idx_list = [c[0] for c in CITIES]
city_w = [c[4] for c in CITIES]
base_by_id = {}
for cid in range(1, N_CUSTOMERS_BASE + N_CUSTOMER_DUPES + 1):
    if cid <= N_CUSTOMERS_BASE:
        gender = rng.choices(["Female", "Male", "Non-binary"], weights=[48.5, 50, 1.5])[0]
        first = rng.choice(FIRST_F if gender == "Female" else FIRST_M if gender == "Male" else FIRST_F + FIRST_M)
        last = rng.choice(LAST)
        email = f"{first}.{last}{rng.randint(1, 999)}@{rng.choice(DOMAINS)}".lower()
        dob = D(1955, 1, 1) + dt.timedelta(days=rng.randint(0, 365 * 47))          # born 1955..2002
        city_idx = rng.choices(city_idx_list, weights=city_w)[0]
        signup = D(2022, 1, 1) + dt.timedelta(days=(cid - 1) * 1445 // N_CUSTOMERS_BASE)
        segment = rng.choices(SEGMENTS, weights=SEG_W)[0]
        active = rng.random() > 0.14
        base_by_id[cid] = (gender, first, last, email, dob, city_idx)
    else:  # duplicate person who registered a second account later
        src = rng.randint(1, N_CUSTOMERS_BASE)
        gender, first, last, email, dob, city_idx = base_by_id[src]
        src_signup = D(2022, 1, 1) + dt.timedelta(days=(src - 1) * 1445 // N_CUSTOMERS_BASE)
        signup = min(src_signup + dt.timedelta(days=rng.randint(30, 500)), D(2025, 12, 20))
        segment = rng.choices(SEGMENTS, weights=[70, 20, 8, 2])[0]
        active = True
    _, city, state, country, _, _ = CITIES[city_idx - 1]
    upd = dt.datetime.combine(signup, dt.time(rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59))) \
        + dt.timedelta(days=rng.randint(0, max(0, (END - signup).days)))
    upd = min(upd, dt.datetime(2025, 12, 31, 23, 59, 59))
    gen_rows.append((cid, city_idx, city, state, country, signup, segment, active, gender, email))

    # ---- apply deliberate data-quality problems to the published copy ----
    g_out = gender
    r = rng.random()
    if r < 0.04:
        g_out = {"Female": "F", "Male": "M", "Non-binary": "NB"}[gender]
    elif r < 0.07:
        g_out = gender.lower()
    elif r < 0.09:
        g_out = gender.upper()
    elif r < 0.12:
        g_out = None
    f_out, l_out = first, last
    r = rng.random()
    if r < 0.03:
        f_out = pad(first)
    elif r < 0.05:
        f_out = first.upper()
    elif r < 0.06:
        f_out = first.lower()
    r = rng.random()
    if r < 0.03:
        l_out = pad(last)
    elif r < 0.05:
        l_out = last.upper()
    elif r < 0.06:
        l_out = last.lower()
    e_out = email
    r = rng.random()
    if r < 0.04:
        e_out = None
    elif r < 0.07:
        e_out = email.upper()
    elif r < 0.095:
        e_out = pad(email)
    elif r < 0.101:
        e_out = email.replace("@", "")
    dob_out = dob
    r = rng.random()
    if r < 0.02:
        dob_out = None
    elif r < 0.023:
        dob_out = D(1900, 1, 1)
    elif r < 0.0235:
        dob_out = D(2031, 5, 17)
    c_out = city
    r = rng.random()
    if city in ALT_CITY and r < ALT_CITY[city][1]:
        c_out = ALT_CITY[city][0]
    elif r < 0.04:
        c_out = city.lower()
    elif r < 0.06:
        c_out = city.upper()
    elif r < 0.08:
        c_out = pad(city)
    s_out = state if rng.random() > 0.01 else None
    seg_out = segment
    r = rng.random()
    if r < 0.02:
        seg_out = None
    elif r < 0.04:
        seg_out = segment.lower()
    elif r < 0.05:
        seg_out = segment.upper()
    cust_rows.append((cid, f_out, l_out, e_out, g_out, dob_out, c_out, s_out, country, signup, seg_out, active, upd))

schema_gen = StructType([
    StructField("customer_id", IntegerType()), StructField("city_idx", IntegerType()), StructField("city", StringType()),
    StructField("state", StringType()), StructField("country", StringType()), StructField("signup_date", DateType()),
    StructField("customer_segment", StringType()), StructField("is_active", BooleanType()),
    StructField("gender", StringType()), StructField("email", StringType()),
])
schema_cust = StructType([
    StructField("customer_id", IntegerType()), StructField("first_name", StringType()), StructField("last_name", StringType()),
    StructField("email", StringType()), StructField("gender", StringType()), StructField("date_of_birth", DateType()),
    StructField("city", StringType()), StructField("state", StringType()), StructField("country", StringType()),
    StructField("signup_date", DateType()), StructField("customer_segment", StringType()), StructField("is_active", BooleanType()),
    StructField("updated_at", TimestampType()),
])
spark.createDataFrame(gen_rows, schema_gen).write.format(FMT.lower()).mode("overwrite").saveAsTable("_gen_customers")
spark.createDataFrame(cust_rows, schema_cust).createOrReplaceTempView("v_customers")
run("INSERT INTO customers SELECT * FROM v_customers")

# clean city lookup used when generating shipping addresses
spark.createDataFrame([(c[0], c[1], c[2], c[3], c[5]) for c in CITIES],
                      StructType([StructField("idx", IntegerType()), StructField("city", StringType()), StructField("state", StringType()),
                                  StructField("country", StringType()), StructField("pin_prefix", StringType())])
                      ).write.format(FMT.lower()).mode("overwrite").saveAsTable("_gen_cities")
print("customers:", spark.table("customers").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Inventory and warehouses stock (seeded Python)

# COMMAND ----------

inv_rows, inv_id = [], 1
for pid in range(1, N_PRODUCTS + 1):
    discontinued = PRODUCTS[pid - 1][7] is False
    if pid > 300 and rng.random() < 0.02:
        continue                                   # ~2% of newer products have no inventory row yet
    whs = sorted(rng.sample(range(1, 13), rng.randint(4, 6)))
    # ~6% of active mid-popularity products are "stock-stressed": only a few days of cover left
    stressed = (not discontinued) and pid <= 800 and rng.random() < 0.06
    est_daily_units = 250 * (1 / 1.8) * (pid / N_PRODUCTS) ** (1 / 1.8 - 1) / N_PRODUCTS
    stress_total = est_daily_units * rng.uniform(1, 40)
    for wh in whs:
        stock = 0 if rng.random() < 0.08 else min(1500, int(rng.expovariate(1 / (25 if discontinued else 90))))
        if stressed:
            stock = int(stress_total / len(whs) * rng.uniform(0.3, 1.7))
        reserved = min(stock, int(stock * rng.random() * 0.3))
        r = rng.random()
        if r < 0.005:
            reserved = stock + rng.randint(1, 10)           # reserved > on-hand
        elif r < 0.008:
            stock = -rng.randint(1, 8)                       # negative stock (sync error)
        reorder = rng.randint(10, 100)
        if rng.random() < 0.72:
            ts = dt.datetime(2025, 12, rng.randint(15, 31), rng.randint(0, 23), rng.randint(0, 59), 0)
        else:
            ts = dt.datetime(2025, rng.randint(5, 12), rng.randint(1, 14), rng.randint(0, 23), rng.randint(0, 59), 0)
        inv_rows.append((inv_id, pid, wh, stock, reserved, reorder, ts))
        inv_id += 1
spark.createDataFrame(inv_rows, StructType([
    StructField("inventory_id", IntegerType()), StructField("product_id", IntegerType()), StructField("warehouse_id", IntegerType()),
    StructField("stock_quantity", IntegerType()), StructField("reserved_quantity", IntegerType()),
    StructField("reorder_level", IntegerType()), StructField("last_updated_at", TimestampType())])
).createOrReplaceTempView("v_inventory")
run("INSERT INTO inventory SELECT * FROM v_inventory")
print("inventory:", spark.table("inventory").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Orders skeleton (set-based SQL)
# MAGIC
# MAGIC * 12 % of customers never buy (browse-only). Buyers have a behaviour class: 60 % casual (low order propensity),
# MAGIC   30 % regular (4x) and 10 % loyal (30x) - so many customers order once or twice while a few order dozens of times.
# MAGIC * About 35 % of customers "churn": they only order during a short window after sign-up.
# MAGIC * An order is never placed before the customer's sign-up date.
# MAGIC * ~0.25 % of orders are accidental **double submissions** (same customer, same basket, seconds apart).

# COMMAND ----------

run(f"""
CREATE TABLE _gen_buyers USING {FMT} AS
SELECT customer_id, signup_date, active_until,
       CASE WHEN cls < 60 THEN 1 WHEN cls < 90 THEN 4 ELSE 30 END AS w   -- order propensity weight
FROM (
  SELECT customer_id, signup_date, {H('customer_id','cls',100)} AS cls,
         least(DATE'{SNAPSHOT}', date_add(signup_date,
               CASE WHEN {U('customer_id','life')} < 0.35 THEN 20 + CAST({U('customer_id','life2')} * 340 AS INT) ELSE 5000 END)) AS active_until
  FROM _gen_customers
  WHERE {H('customer_id','buyer',100)} >= 12
)
""")
# one "ticket" per unit of propensity; orders draw tickets uniformly -> weighted customer choice
run(f"""
CREATE TABLE _gen_tickets USING {FMT} AS
SELECT row_number() OVER (ORDER BY b.customer_id, t.i) AS k, b.customer_id, b.signup_date, b.active_until
FROM _gen_buyers b LATERAL VIEW explode(sequence(1, b.w)) t AS i
""")
N_BUYERS = spark.table("_gen_buyers").count()
N_TICKETS = spark.table("_gen_tickets").count()
print("buyers:", N_BUYERS, "| tickets:", N_TICKETS)

run(f"""
CREATE TABLE _gen_orders_a USING {FMT} AS
WITH nums AS (SELECT CAST(id + 1 AS BIGINT) AS n FROM range(0, {N_ORDERS_REGULAR})),
p AS (
  SELECT nums.n, b.customer_id, b.signup_date, b.active_until, c.city_idx
  FROM nums
  JOIN _gen_tickets b ON b.k = 1 + CAST(floor({N_TICKETS} * {U('nums.n','cust')}) AS INT)
  JOIN _gen_customers c ON c.customer_id = b.customer_id
),
d1 AS (
  SELECT *, date_add(signup_date, CAST(floor((datediff(active_until, signup_date) + 1) * pow({U('n','day')}, 0.9)) AS INT)) AS base_day,
            make_date(1, 1, 1) AS dummy
  FROM p
),
d2 AS (  -- 14% of orders are pulled into the Oct-Dec festive season of their year (if the customer was active then)
  SELECT *, make_date(year(base_day), 10 + {H('n','sm',3)}, 1 + {H('n','sd',28)}) AS festive_day FROM d1
),
d3 AS (
  SELECT *, CASE WHEN {U('n','season')} < 0.14 AND festive_day >= signup_date AND festive_day <= active_until
                 THEN festive_day ELSE base_day END AS order_day FROM d2
),
d4 AS (
  SELECT *, CASE WHEN {U('n','tod')} < 0.65 THEN 36000 + CAST({U('n','tod2')} * 43200 AS INT)
                 ELSE CAST({U('n','tod3')} * 86400 AS INT) END AS sec_of_day,
            datediff(DATE'{SNAPSHOT}', order_day) AS age_days,
            {U('n','status')} AS u_status,
            {U('n','pm')} AS u_pm
  FROM d3
),
d5 AS (
  SELECT *,
    CASE
      WHEN age_days <= 3  THEN CASE WHEN u_status < 0.30 THEN 'Pending' WHEN u_status < 0.65 THEN 'Processing' WHEN u_status < 0.90 THEN 'Shipped' ELSE 'Cancelled' END
      WHEN age_days <= 10 THEN CASE WHEN u_status < 0.05 THEN 'Processing' WHEN u_status < 0.30 THEN 'Shipped' WHEN u_status < 0.40 THEN 'Cancelled' ELSE 'Delivered' END
      ELSE CASE WHEN u_status < 0.040 THEN 'Returned' WHEN u_status < 0.120 THEN 'Cancelled' WHEN u_status < 0.150 THEN 'Failed'
                WHEN u_status < 0.160 THEN 'Processing' WHEN u_status < 0.170 THEN 'Pending' WHEN u_status < 0.175 THEN 'Shipped'
                ELSE 'Delivered' END
    END AS order_status,
    CASE WHEN u_pm < 0.34 THEN 'UPI' WHEN u_pm < 0.56 THEN 'Credit Card' WHEN u_pm < 0.70 THEN 'Debit Card'
         WHEN u_pm < 0.77 THEN 'Net Banking' WHEN u_pm < 0.92 THEN 'Cash on Delivery' WHEN u_pm < 0.98 THEN 'Wallet'
         ELSE 'Gift Card' END AS pm0
  FROM d4
)
SELECT n, customer_id, city_idx,
       timestamp_seconds(unix_timestamp(CAST(order_day AS TIMESTAMP)) + sec_of_day) AS order_ts,
       order_status,
       CASE WHEN order_status = 'Failed' AND pm0 = 'Cash on Delivery' THEN 'UPI' ELSE pm0 END AS payment_method
FROM d5
""")

# add shipping address text + final status/ids; append the accidental double-submissions
run(f"""
CREATE TABLE _gen_orders USING {FMT} AS
WITH base AS (
  SELECT a.n, a.n AS seed_n, a.customer_id, a.order_ts, a.order_status, a.payment_method, a.city_idx FROM _gen_orders_a a
),
dups AS (
  SELECT {N_ORDERS_REGULAR} + row_number() OVER (ORDER BY a.n) AS n, a.n AS seed_n, a.customer_id,
         timestamp_seconds(unix_timestamp(a.order_ts) + 10 + {H('a.n','dupgap',170)}) AS order_ts,
         CASE WHEN {U('a.n','dupcancel')} < 0.5 THEN 'Cancelled' ELSE a.order_status END AS order_status,
         a.payment_method, a.city_idx
  FROM _gen_orders_a a WHERE {U('a.n','dupsel')} < 0.0025
),
allo AS (SELECT * FROM base UNION ALL SELECT * FROM dups),
ided AS (
  SELECT 100000 + row_number() OVER (ORDER BY order_ts, n) AS order_id, *
  FROM allo
),
ship AS (
  SELECT i.*, CASE WHEN {U('i.n','shipcity')} < 0.85 THEN i.city_idx ELSE 1 + {H('i.n','shipcity2',21)} END AS ship_idx
  FROM ided i
)
SELECT s.order_id, s.n, s.seed_n, s.customer_id, s.order_ts, s.order_status, s.payment_method,
       concat(CAST(1 + {H('s.n','hno',998)} AS STRING), ', ',
              element_at(array('MG Road','Station Road','Park Street','Gandhi Nagar','Residency Road','Lake View Colony','Civil Lines','Nehru Place','Sector 14','Hill Road'), 1 + {H('s.n','street',10)}),
              ', ', ci.city, ', ', ci.state, ' ', ci.pin_prefix, lpad(CAST({H('s.n','pin',1000)} AS STRING), 3, '0')) AS shipping_address,
       -- last status change: depends on the final status
       least(timestamp'{SNAPSHOT} 23:59:59',
         timestamp_seconds(unix_timestamp(s.order_ts) +
           CASE s.order_status
             WHEN 'Delivered'  THEN 172800 + {H('s.n','upd',518400)}
             WHEN 'Returned'   THEN 864000 + {H('s.n','upd',1728000)}
             WHEN 'Cancelled'  THEN 3600 + {H('s.n','upd',169200)}
             WHEN 'Failed'     THEN 60 + {H('s.n','upd',900)}
             WHEN 'Shipped'    THEN 86400 + {H('s.n','upd',86400)}
             WHEN 'Processing' THEN 3600 + {H('s.n','upd',43200)}
             ELSE 0 END)) AS updated_at
FROM ship s JOIN _gen_cities ci ON ci.idx = s.ship_idx
""")
print("orders skeleton:", spark.table("_gen_orders").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Order items, order totals and final `orders` / `order_items`

# COMMAND ----------

run(f"""
CREATE TABLE _gen_items_base USING {FMT} AS
WITH cnt AS (
  SELECT order_id, seed_n, order_ts,
         CASE WHEN u < 0.35 THEN 1 WHEN u < 0.65 THEN 2 WHEN u < 0.82 THEN 3 WHEN u < 0.91 THEN 4
              WHEN u < 0.96 THEN 5 WHEN u < 0.985 THEN 6 ELSE 7 END AS n_items
  FROM (SELECT *, {U('seed_n','nitems')} AS u FROM _gen_orders)
),
lines AS (
  SELECT cnt.order_id, cnt.seed_n, cnt.order_ts, ln AS line_no, cnt.seed_n * 10 + ln AS k
  FROM cnt LATERAL VIEW explode(sequence(1, n_items)) t AS ln
),
pick1 AS (
  SELECT l.*, 1 + CAST(floor({N_PRODUCTS} * pow({U('l.k','prod')}, 1.8)) AS INT) AS p1 FROM lines l
),
pick2 AS (
  SELECT x.*,
    CASE WHEN (({U('x.k','baditem')} < 0.003)   -- ~0.3% of lines ignore launch/discontinue rules (data issue)
              OR (pr.launch_date <= to_date(x.order_ts) AND (pr.discontinued_date IS NULL OR pr.discontinued_date >= to_date(x.order_ts))))
              AND NOT (pr.product_id > 300 AND {H('pr.product_id','dead',1000)} < 40)   -- ~4% of the catalogue never sells ("dead stock")
         THEN x.p1 ELSE 1 + {H('x.k','prod2',300)} END AS product_id
  FROM pick1 x JOIN products pr ON pr.product_id = x.p1
),
priced AS (
  SELECT y.order_id, y.line_no, y.k, y.product_id, y.order_ts,
    CASE WHEN {U('y.k','typo')} < 0.0003 THEN 10 ELSE 1 END AS price_mult,
    round(pr.selling_price * (1 - 0.03 * (2025 - year(y.order_ts))) * (0.98 + 0.04 * {U('y.k','pf')}), 2) AS base_price,
    CASE WHEN {U('y.k','qty')} < 0.75 THEN 1 WHEN {U('y.k','qty')} < 0.93 THEN 2 WHEN {U('y.k','qty')} < 0.98 THEN 3
         ELSE 4 + {H('y.k','qty2',3)} END AS q0,
    {U('y.k','bulk')} AS u_bulk,
    elt(1 + {H('y.k','disc',10)}, 0, 0, 0, 0, 5, 10, 10, 15, 20, 25) / 100.0 AS disc_pct
  FROM pick2 y JOIN products pr ON pr.product_id = y.product_id
),
q AS (
  SELECT *, CAST(base_price * price_mult AS DECIMAL(12,2)) AS unit_price,
         CASE WHEN u_bulk < 0.0003 THEN 10 + {H('k','bulkq',15)}
              WHEN base_price < 600 THEN q0 + {H('k','cheapq',3)}
              ELSE q0 END AS quantity
  FROM priced
)
SELECT order_id, line_no, k, product_id, CAST(quantity AS INT) AS quantity, unit_price,
       CAST(round(quantity * unit_price * disc_pct, 2) AS DECIMAL(12,2)) AS discount,
       CAST(round(quantity * unit_price - round(quantity * unit_price * disc_pct, 2), 2) AS DECIMAL(12,2)) AS line_total
FROM q
""")

# order totals (clean) derived from ALL item lines
run(f"""
CREATE TABLE _gen_order_totals USING {FMT} AS
SELECT order_id, subtotal, disc,
       CAST(round(subtotal * 0.18, 2) AS DECIMAL(12,2)) AS tax,
       CAST(CASE WHEN subtotal >= 999 THEN 0 WHEN subtotal >= 500 THEN 49 ELSE 99 END AS DECIMAL(12,2)) AS ship,
       CAST(subtotal + round(subtotal * 0.18, 2) + CASE WHEN subtotal >= 999 THEN 0 WHEN subtotal >= 500 THEN 49 ELSE 99 END AS DECIMAL(12,2)) AS total_clean
FROM (SELECT order_id, CAST(sum(line_total) AS DECIMAL(14,2)) AS subtotal, CAST(sum(discount) AS DECIMAL(14,2)) AS disc
      FROM _gen_items_base GROUP BY order_id)
""")

# final order_items: base lines (minus 'lost' lines for ~0.1% of orders) + ~0.3% duplicated lines
run("""
INSERT INTO order_items
SELECT row_number() OVER (ORDER BY order_id, line_no, is_dup) AS order_item_id,
       order_id, product_id, quantity, unit_price, discount, line_total
FROM (
  SELECT order_id, line_no, 0 AS is_dup, product_id, quantity, unit_price, discount, line_total, k FROM _gen_items_base
  UNION ALL
  SELECT order_id, line_no, 1 AS is_dup, product_id, quantity, unit_price, discount, line_total, k FROM _gen_items_base
  WHERE """ + U('k', 'dupline') + """ < 0.003
) x
WHERE NOT (""" + H('order_id', 'noitems', 2400) + """ = 0)
""")

# final orders (adds header-level quality problems)
run(f"""
INSERT INTO orders
SELECT o.order_id, o.customer_id, o.order_ts,
       o.order_status,
       CASE WHEN {U('o.n','addrnull')} < 0.02 THEN NULL ELSE o.shipping_address END,
       CASE WHEN {U('o.n','pmdq')} < 0.03 THEN lower(o.payment_method)
            WHEN {U('o.n','pmdq')} < 0.045 THEN concat(o.payment_method, ' ')
            ELSE o.payment_method END,
       CASE WHEN {U('o.n','totbad')} < 0.003 THEN CAST(round(t.total_clean * (1 + (CASE WHEN {U('o.n','totsign')} < 0.5 THEN -1 ELSE 1 END) * (0.05 + 0.15 * {U('o.n','totmag')})), 2) AS DECIMAL(12,2))
            ELSE t.total_clean END,
       CASE WHEN {U('o.n','discnull')} < 0.005 THEN NULL ELSE t.disc END,
       t.tax, t.ship,
       o.updated_at,
       CASE WHEN {U('o.n','late')} < 0.03 THEN timestamp_seconds(unix_timestamp(o.updated_at) + 259200 + {H('o.n','latedays',1468800)})
            ELSE timestamp_seconds(unix_timestamp(o.updated_at) + 30 + {H('o.n','loadsec',3600)}) END
FROM _gen_orders o JOIN _gen_order_totals t ON t.order_id = o.order_id
""")
print("orders:", spark.table("orders").count(), "| order_items:", spark.table("order_items").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Payments
# MAGIC
# MAGIC Retries (failed attempt then success), refunds on cancelled prepaid orders, COD collected on delivery,
# MAGIC duplicate gateway callbacks, a handful of short payments and some orders that have no payment row at all.

# COMMAND ----------

run(f"""
CREATE TABLE _gen_pay USING {FMT} AS
WITH o AS (
  SELECT g.order_id, g.n, g.customer_id, g.order_ts, g.order_status, g.payment_method, g.updated_at, t.total_clean,
         (g.payment_method <> 'Cash on Delivery') AS prepaid,
         ({H('g.customer_id','tester',300)} = 7) AS tester
  FROM _gen_orders g JOIN _gen_order_totals t ON t.order_id = g.order_id
),
s AS (
  SELECT *,
    CASE
      WHEN order_status IN ('Delivered','Returned') THEN CASE WHEN {U('n','nopay')} < 0.003 THEN NULL ELSE 'Success' END
      WHEN order_status IN ('Shipped','Processing') THEN CASE WHEN prepaid THEN 'Success' ELSE 'Pending' END
      WHEN order_status = 'Pending' THEN 'Pending'
      WHEN order_status = 'Failed' THEN 'Failed'
      WHEN order_status = 'Cancelled' THEN CASE WHEN NOT prepaid THEN NULL WHEN {U('n','cx')} < 0.60 THEN 'Refunded'
                                                WHEN {U('n','cx')} < 0.78 THEN 'Failed' ELSE NULL END
    END AS final_status
  FROM o
),
f AS (
  SELECT *, CASE WHEN final_status IN ('Success','Refunded') AND prepaid
                 THEN CASE WHEN tester THEN 2 + {H('n','nfail',4)} WHEN {U('n','retry')} < 0.04 THEN 1 ELSE 0 END
                 WHEN final_status = 'Failed' THEN {H('n','nfail2',3)}
                 ELSE 0 END AS n_fail
  FROM s WHERE final_status IS NOT NULL
),
att AS (
  SELECT f.*, attempt FROM f LATERAL VIEW explode(sequence(1, n_fail + 1)) t AS attempt
),
rows AS (
  SELECT order_id, n, customer_id, attempt, n_fail,
    CASE WHEN attempt <= n_fail THEN 'Failed' ELSE final_status END AS payment_status,
    CASE WHEN attempt <= n_fail AND {U('n * 10 + attempt','pmswitch')} < 0.35
         THEN elt(1 + {H('n * 10 + attempt','pmx',5)}, 'UPI','Credit Card','Debit Card','Net Banking','Wallet')
         ELSE payment_method END AS payment_method,
    CASE WHEN NOT prepaid AND final_status = 'Success' THEN updated_at
         ELSE timestamp_seconds(unix_timestamp(order_ts) + 30 + {H('n','paydelay',600)} + (attempt - 1) * (60 + {H('n * 10 + attempt','retrygap',240)})) END AS payment_date,
    CASE WHEN attempt > n_fail AND final_status = 'Success' AND {U('n','short')} < 0.003
         THEN CAST(round(total_clean * (0.80 + 0.19 * {U('n','shortmag')}), 2) AS DECIMAL(12,2))
         ELSE total_clean END AS payment_amount,
    CASE WHEN {H('order_id','refreuse',5000)} = 7 AND attempt = n_fail + 1
         THEN concat('TXN', lpad(CAST(pmod(xxhash64(order_id - 1, 1, 'ref'), 100000000000) AS STRING), 11, '0'))
         ELSE concat('TXN', lpad(CAST(pmod(xxhash64(order_id, attempt, 'ref'), 100000000000) AS STRING), 11, '0')) END AS transaction_reference,
    0 AS is_dup
  FROM att
)
SELECT * FROM rows
UNION ALL   -- duplicate gateway callbacks: same reference, new row, a few seconds later
SELECT order_id, n, customer_id, attempt, n_fail, payment_status, payment_method,
       timestamp_seconds(unix_timestamp(payment_date) + 2 + {H('n','dupsec',20)}) AS payment_date,
       payment_amount, transaction_reference, 1 AS is_dup
FROM rows WHERE payment_status = 'Success' AND {U('n','dupcb')} < 0.004
""")

run(f"""
INSERT INTO payments
SELECT row_number() OVER (ORDER BY payment_date, order_id, attempt, is_dup) AS payment_id,
       order_id, payment_date,
       CASE WHEN {U('n * 10 + attempt','pmdq2')} < 0.03 THEN upper(payment_method)
            WHEN {U('n * 10 + attempt','pmdq2')} < 0.045 THEN concat(' ', payment_method) ELSE payment_method END,
       CASE WHEN {U('n * 10 + attempt','psdq')} < 0.015 THEN lower(payment_status)
            WHEN {U('n * 10 + attempt','psdq')} < 0.020 THEN upper(payment_status) ELSE payment_status END,
       payment_amount, transaction_reference
FROM _gen_pay
""")
print("payments:", spark.table("payments").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 · Returns (line level, raised days after the purchase)

# COMMAND ----------

run(f"""
INSERT INTO returns
SELECT row_number() OVER (ORDER BY return_date, order_item_id) AS return_id,
       order_item_id, return_date,
       CASE WHEN {U('order_item_id','rrnull')} < 0.03 THEN NULL
            WHEN {U('order_item_id','rrdq')} < 0.02 THEN lower(reason) ELSE reason END AS return_reason,
       returned_quantity,
       CASE WHEN return_status = 'Rejected' THEN CAST(0 AS DECIMAL(12,2))
            WHEN return_status = 'Pending' THEN NULL
            WHEN {U('order_item_id','refbig')} < 0.004 THEN CAST(round(refund_base * 1.6, 2) AS DECIMAL(12,2))
            ELSE CAST(round(refund_base, 2) AS DECIMAL(12,2)) END AS refund_amount,
       return_status
FROM (
  SELECT x.*,
    CASE WHEN rr < 0.22 THEN 'Defective Product' WHEN rr < 0.30 THEN 'Wrong Item Delivered' WHEN rr < 0.48 THEN 'Size/Fit Issue'
         WHEN rr < 0.64 THEN 'Not as Described' WHEN rr < 0.79 THEN 'Changed Mind' WHEN rr < 0.84 THEN 'Late Delivery'
         WHEN rr < 0.94 THEN 'Damaged in Transit' ELSE 'Better Price Elsewhere' END AS reason,
    CASE WHEN rs_age <= 5 THEN CASE WHEN rs < 0.5 THEN 'Pending' ELSE 'Approved' END
         ELSE CASE WHEN rs < 0.82 THEN 'Refunded' WHEN rs < 0.92 THEN 'Rejected' ELSE 'Approved' END END AS return_status,
    line_total * returned_quantity / quantity * CASE WHEN rr >= 0.64 AND rr < 0.79 THEN 0.9 ELSE 1.0 END AS refund_base
  FROM (
    SELECT i.order_item_id, i.quantity, i.line_total, o.order_status,
      least(timestamp'{SNAPSHOT} 23:00:00', timestamp_seconds(unix_timestamp(o.order_date) +
         86400 * (CASE WHEN {U('i.order_item_id','late')} < 0.06 THEN 22 + {H('i.order_item_id','lated',39)} ELSE 3 + {H('i.order_item_id','retd',19)} END)
         + {H('i.order_item_id','rets',86000)})) AS return_date,
      CASE WHEN {U('i.order_item_id','rq')} < 0.80 THEN i.quantity
           WHEN {U('i.order_item_id','rq')} < 0.997 THEN 1 + {H('i.order_item_id','rq2',100)} % i.quantity
           ELSE i.quantity + 1 + {H('i.order_item_id','rq3',2)} END AS returned_quantity,
      {U('i.order_item_id','rr')} AS rr, {U('i.order_item_id','rs')} AS rs,
      datediff(DATE'{SNAPSHOT}', to_date(least(timestamp'{SNAPSHOT} 23:00:00', timestamp_seconds(unix_timestamp(o.order_date) + 86400 * (3 + {H('i.order_item_id','retd',19)}))))) AS rs_age
    FROM order_items i JOIN orders o ON o.order_id = i.order_id
    WHERE (o.order_status = 'Returned' AND ({U('i.order_item_id','rpick')} < 0.65 OR i.order_item_id = (SELECT min(order_item_id) FROM order_items m WHERE m.order_id = i.order_id)))
       OR (o.order_status = 'Delivered' AND {U('i.order_item_id','rpick')} < 0.035)
  ) x
) y
""")
print("returns:", spark.table("returns").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 · Order status history (event log of every status transition)

# COMMAND ----------

run(f"""
INSERT INTO order_status_history
SELECT row_number() OVER (ORDER BY status_timestamp, order_id, pos) AS history_id, order_id, order_status, status_timestamp,
       CASE order_status WHEN 'Pending' THEN 'checkout-service' WHEN 'Processing' THEN 'warehouse-system'
            WHEN 'Shipped' THEN 'logistics-partner' WHEN 'Delivered' THEN 'logistics-partner'
            WHEN 'Returned' THEN 'returns-team' WHEN 'Cancelled' THEN CASE WHEN {H('order_id','cb',2)} = 0 THEN 'customer' ELSE 'support-agent' END
            ELSE 'payment-gateway' END AS changed_by
FROM (
  SELECT order_id, pos, st AS order_status,
         timestamp_seconds(unix_timestamp(order_ts) + floor((unix_timestamp(updated_at) - unix_timestamp(order_ts)) * pos / greatest(size(steps) - 1, 1))
                           + CASE WHEN pos = 0 THEN 0 ELSE {H('order_id * 10 + pos','jit',120)} END) AS status_timestamp
  FROM (
    SELECT order_id, order_ts, updated_at,
      CASE order_status
        WHEN 'Pending'    THEN array('Pending')
        WHEN 'Processing' THEN array('Pending','Processing')
        WHEN 'Shipped'    THEN array('Pending','Processing','Shipped')
        WHEN 'Delivered'  THEN array('Pending','Processing','Shipped','Delivered')
        WHEN 'Returned'   THEN array('Pending','Processing','Shipped','Delivered','Returned')
        WHEN 'Cancelled'  THEN CASE WHEN {H('order_id','cx2',2)} = 0 THEN array('Pending','Processing','Cancelled') ELSE array('Pending','Cancelled') END
        ELSE array('Pending','Failed') END AS steps
    FROM _gen_orders
  ) LATERAL VIEW posexplode(steps) e AS pos, st
)
""")
print("order_status_history:", spark.table("order_status_history").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 · Customer activity (sessions → funnel-shaped events)

# COMMAND ----------

run(f"""
CREATE TABLE _gen_sessions USING {FMT} AS
WITH s AS (SELECT CAST(id + 1 AS BIGINT) AS s FROM range(0, {N_SESSIONS})),
x AS (
  SELECT s, CASE WHEN {U('s','anon')} < 0.22 THEN NULL ELSE 1 + CAST(floor(12000 * pow({U('s','cust')}, 1.2)) AS INT) END AS customer_id FROM s
),
y AS (
  SELECT x.s, x.customer_id,
    CASE WHEN x.customer_id IS NULL THEN date_add(DATE'2022-01-01', CAST(floor(1460 * pow({U('x.s','adate')}, 0.8)) AS INT))
         ELSE date_add(c.signup_date, CAST(floor((datediff(DATE'{SNAPSHOT}', c.signup_date) + 1) * pow({U('x.s','sday')}, 0.8)) AS INT)) END AS session_day
  FROM x LEFT JOIN _gen_customers c ON c.customer_id = x.customer_id
)
SELECT s, customer_id, session_day,
       timestamp_seconds(unix_timestamp(CAST(session_day AS TIMESTAMP)) + CASE WHEN {U('s','h1')} < 0.7 THEN 28800 + CAST({U('s','h2')} * 50400 AS INT) ELSE CAST({U('s','h3')} * 86400 AS INT) END) AS session_start,
       CASE WHEN {U('s','dev')} < 0.58 THEN 'mobile' WHEN {U('s','dev')} < 0.88 THEN 'desktop' WHEN {U('s','dev')} < 0.96 THEN 'tablet' ELSE 'app' END AS device_type,
       {U('s','stage')} AS u_stage
FROM y
""")

run(f"""
INSERT INTO customer_activity
WITH sess AS (
  SELECT g.*,
    CASE
      WHEN u_stage < 0.30 THEN array('page_view','page_view','search')
      WHEN u_stage < 0.52 THEN CASE WHEN {H('s','v1',2)} = 0 THEN array('page_view','search','product_view','product_view','page_view')
                                    ELSE array('page_view','search','product_view','wishlist_add','product_view') END
      WHEN u_stage < 0.72 THEN CASE WHEN {H('s','v2',4)} = 0 THEN array('page_view','search','product_view','add_to_cart','remove_from_cart','page_view')
                                    ELSE array('page_view','search','product_view','product_view','add_to_cart','page_view') END
      WHEN u_stage < 0.82 THEN array('page_view','search','product_view','add_to_cart','checkout_start')
      ELSE array('page_view','search','product_view','product_view','add_to_cart','checkout_start','purchase') END AS arr0
  FROM _gen_sessions g
),
ev AS (
  SELECT s, customer_id, session_day, session_start, device_type,
         CASE WHEN customer_id IS NOT NULL AND {U('s','login')} < 0.45 THEN concat(array('login'), arr0) ELSE arr0 END AS arr
  FROM sess
),
ex AS (
  SELECT s, customer_id, session_day, session_start, device_type, pos, activity_type
  FROM ev LATERAL VIEW posexplode(arr) e AS pos, activity_type
),
pr0 AS (
  SELECT ex.*,
    CASE WHEN activity_type IN ('product_view','wishlist_add','add_to_cart','remove_from_cart')
         THEN 1 + CAST(floor({N_PRODUCTS} * pow({U("CASE WHEN activity_type = 'product_view' THEN s * 100 + pos ELSE s END", 'prod')}, 1.8)) AS INT) END AS p_raw
  FROM ex
),
pr1 AS (
  SELECT a.*, CASE WHEN a.p_raw IS NULL THEN NULL
                   WHEN p.launch_date <= a.session_day THEN a.p_raw
                   ELSE 1 + {H('a.s * 100 + a.pos','pfb',300)} END AS product_id
  FROM pr0 a LEFT JOIN products p ON p.product_id = a.p_raw
),
final AS (
  SELECT s, customer_id, pos, activity_type, concat('SES-', lpad(CAST(s AS STRING), 7, '0')) AS session_id, device_type,
         timestamp_seconds(unix_timestamp(session_start) + pos * 40 + {H('s * 100 + pos','gap',30)}) AS activity_timestamp,
         product_id, 0 AS is_dup
  FROM pr1
),
withdup AS (
  SELECT * FROM final
  UNION ALL
  SELECT s, customer_id, pos, activity_type, session_id, device_type, activity_timestamp, product_id, 1 AS is_dup
  FROM final WHERE {U('s * 100 + pos','dupev')} < 0.003
)
SELECT row_number() OVER (ORDER BY activity_timestamp, session_id, pos, is_dup) AS activity_id,
       customer_id, activity_timestamp, activity_type, session_id,
       CASE WHEN {U('s * 100 + pos','devnull')} < 0.01 THEN NULL
            WHEN {U('s','devdq')} < 0.04 THEN initcap(device_type)
            WHEN {U('s','devdq')} < 0.06 THEN upper(device_type) ELSE device_type END AS device_type,
       product_id
FROM withdup
""")
print("customer_activity:", spark.table("customer_activity").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13 · `customer_changes` (CDC feed), `dim_date`, `orders_incoming_batch`

# COMMAND ----------

# ---------- customer_changes: a version history that ends in each customer's current clean attributes ----------
CITY_BY_IDX = {c[0]: c for c in CITIES}
india_idx = [c[0] for c in CITIES if c[3] == "India"]
TIER = {"Bronze": 0, "Silver": 1, "Gold": 2, "Platinum": 3}
chg_rows, cid_counter = [], 1
gen_by_id = {g[0]: g for g in gen_rows}
for cid, g in gen_by_id.items():
    _, city_idx, city, state, country, signup, segment, active = g[:8]
    start_ts = dt.datetime.combine(signup, dt.time(rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59)))
    n_changes = 0
    if rng.random() < 0.30 and signup < D(2025, 6, 1):
        n_changes = rng.choices([1, 2, 3], weights=[55, 30, 15])[0]
    # versions are built backwards from the customer's current state
    cur = (city_idx, segment, active)
    versions = [cur]
    for _ in range(n_changes):
        ci, sg, ac = versions[0]
        if rng.random() < 0.6:
            alt = rng.choice([i for i in india_idx if i != ci]) if CITY_BY_IDX[ci][3] == "India" else rng.choice([i for i in range(1, 22) if i != ci])
            versions.insert(0, (alt, sg, True))
        else:
            lower = [s for s in SEGMENTS if s != sg]
            versions.insert(0, (ci, rng.choice(lower), True))
    if not active and len(versions) == 1:
        versions.insert(0, (city_idx, segment, True))   # an active version precedes deactivation
    n_v = len(versions)
    horizon = max(1, (D(2025, 12, 31) - signup).days)
    cut_days = sorted(rng.sample(range(5, max(horizon, 6 + n_v)), n_v - 1)) if n_v > 1 else []
    for vi, (ci, sg, ac) in enumerate(versions):
        ts = start_ts if vi == 0 else dt.datetime.combine(signup + dt.timedelta(days=cut_days[vi - 1]), dt.time(rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59)))
        ts = min(ts, dt.datetime(2025, 12, 31, 23, 59, 59))
        late = rng.random() < 0.03
        recv = ts + (dt.timedelta(days=rng.randint(3, 30)) if late else dt.timedelta(seconds=rng.randint(5, 900)))
        c = CITY_BY_IDX[ci]
        row = (ts, recv, "INSERT" if vi == 0 else "UPDATE", c[1], c[2], sg, ac, cid)
        chg_rows.append(row)
        if rng.random() < 0.004:   # exact duplicate delivery of the same change
            chg_rows.append(row)
chg_rows.sort(key=lambda r: (r[0], r[7]))
chg_rows = [(i + 1, r[7], r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for i, r in enumerate(chg_rows)]
spark.createDataFrame(chg_rows, StructType([
    StructField("change_id", LongType()), StructField("customer_id", IntegerType()), StructField("change_timestamp", TimestampType()),
    StructField("received_at", TimestampType()), StructField("change_type", StringType()), StructField("city", StringType()),
    StructField("state", StringType()), StructField("customer_segment", StringType()), StructField("is_active", BooleanType())])
).createOrReplaceTempView("v_changes")
run("INSERT INTO customer_changes SELECT * FROM v_changes")
print("customer_changes:", spark.table("customer_changes").count())

# ---------- dim_date ----------
HOLIDAYS = [("2022-01-26", "Republic Day"), ("2023-01-26", "Republic Day"), ("2024-01-26", "Republic Day"), ("2025-01-26", "Republic Day"), ("2026-01-26", "Republic Day"),
            ("2021-12-25", "Christmas"), ("2022-12-25", "Christmas"), ("2023-12-25", "Christmas"), ("2024-12-25", "Christmas"), ("2025-12-25", "Christmas"), ("2026-12-25", "Christmas"),
            ("2022-08-15", "Independence Day"), ("2023-08-15", "Independence Day"), ("2024-08-15", "Independence Day"), ("2025-08-15", "Independence Day"), ("2026-08-15", "Independence Day"),
            ("2022-10-02", "Gandhi Jayanti"), ("2023-10-02", "Gandhi Jayanti"), ("2024-10-02", "Gandhi Jayanti"), ("2025-10-02", "Gandhi Jayanti"), ("2026-10-02", "Gandhi Jayanti"),
            ("2022-10-24", "Diwali"), ("2023-11-12", "Diwali"), ("2024-11-01", "Diwali"), ("2025-10-20", "Diwali"), ("2026-11-08", "Diwali"),
            ("2022-03-18", "Holi"), ("2023-03-08", "Holi"), ("2024-03-25", "Holi"), ("2025-03-14", "Holi"), ("2026-03-04", "Holi")]
spark.createDataFrame([(D.fromisoformat(d), n) for d, n in HOLIDAYS],
                      StructType([StructField("hol_date", DateType()), StructField("hol_name", StringType())])).createOrReplaceTempView("v_holidays")
run("""
INSERT INTO dim_date
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT), d, year(d), quarter(d), month(d), date_format(d, 'MMMM'), weekofyear(d),
       ((dayofweek(d) + 5) % 7) + 1, date_format(d, 'EEEE'),
       ((dayofweek(d) + 5) % 7) + 1 >= 6, h.hol_name IS NOT NULL, h.hol_name,
       year(d) + CASE WHEN month(d) >= 4 THEN 1 ELSE 0 END,
       CAST(floor(((month(d) - 4 + 12) % 12) / 3) + 1 AS INT)
FROM (SELECT explode(sequence(DATE'2021-12-01', DATE'2026-12-31', INTERVAL 1 DAY)) AS d) x
LEFT JOIN v_holidays h ON h.hol_date = x.d
""")
print("dim_date:", spark.table("dim_date").count())

# ---------- orders_incoming_batch ----------
run(f"""
INSERT INTO orders_incoming_batch
WITH upd AS (   -- (a) changed orders: status moved forward after the snapshot
  SELECT o.order_id, o.customer_id, o.order_date,
         CASE o.order_status WHEN 'Pending' THEN 'Processing' WHEN 'Processing' THEN 'Shipped' WHEN 'Shipped' THEN 'Delivered'
              WHEN 'Delivered' THEN 'Returned' ELSE o.order_status END AS order_status,
         o.shipping_address, o.payment_method, o.total_amount, o.discount_amount, o.tax_amount, o.shipping_amount,
         timestamp_seconds(unix_timestamp(timestamp'2026-01-03 06:00:00') + {H('o.order_id','updt',345600)}) AS updated_at
  FROM orders o
  WHERE (o.order_status IN ('Pending','Processing','Shipped') AND {U('o.order_id','inc')} < 0.6)
     OR (o.order_status = 'Delivered' AND {U('o.order_id','inc2')} < 0.004)
),
newo AS (       -- (b) brand-new January orders; ~2% reference customers that are not in `customers` yet
  SELECT 200000 + id + 1 AS order_id,
         CASE WHEN {U('id','nc')} < 0.02 THEN 12001 + {H('id','nc2',50)} ELSE 1 + {H('id','ncust',12000)} END AS customer_id,
         timestamp_seconds(unix_timestamp(timestamp'2026-01-01 00:00:00') + {H('id','nod',600000)}) AS order_date,
         CASE WHEN {U('id','nst')} < 0.5 THEN 'Pending' WHEN {U('id','nst')} < 0.85 THEN 'Processing' ELSE 'Cancelled' END AS order_status,
         CAST(1 + {H('id','nh',998)} AS STRING) || ', MG Road, Mumbai, Maharashtra 400001' AS shipping_address,
         elt(1 + {H('id','npm',5)}, 'UPI','Credit Card','Debit Card','Cash on Delivery','Wallet') AS payment_method,
         CAST(round(300 + pow({U('id','namt')}, 3) * 30000, 2) AS DECIMAL(12,2)) AS total_amount
  FROM range(0, 1500)
),
newo2 AS (
  SELECT order_id, customer_id, order_date, order_status, shipping_address, payment_method, total_amount,
         CAST(round(total_amount * 0.05, 2) AS DECIMAL(12,2)) AS discount_amount,
         CAST(round(total_amount * 0.18 / 1.18, 2) AS DECIMAL(12,2)) AS tax_amount,
         CAST(CASE WHEN total_amount >= 999 THEN 0 ELSE 49 END AS DECIMAL(12,2)) AS shipping_amount,
         timestamp_seconds(unix_timestamp(order_date) + 600 + {H('order_id','nup',86400)}) AS updated_at
  FROM newo
),
late AS (       -- (c) late-arriving old orders: placed in December, only delivered to the warehouse in January
  SELECT 300000 + id + 1 AS order_id, 1 + {H('id','lc',11000)} AS customer_id,
         timestamp_seconds(unix_timestamp(timestamp'2025-12-10 00:00:00') + {H('id','lod',1900000)}) AS order_date,
         'Delivered' AS order_status,
         CAST(1 + {H('id','lh',998)} AS STRING) || ', Park Street, Kolkata, West Bengal 700001' AS shipping_address,
         'UPI' AS payment_method,
         CAST(round(400 + pow({U('id','lamt')}, 3) * 20000, 2) AS DECIMAL(12,2)) AS total_amount
  FROM range(0, 25)
),
late2 AS (
  SELECT order_id, customer_id, order_date, order_status, shipping_address, payment_method, total_amount,
         CAST(round(total_amount * 0.05, 2) AS DECIMAL(12,2)) AS discount_amount,
         CAST(round(total_amount * 0.18 / 1.18, 2) AS DECIMAL(12,2)) AS tax_amount, CAST(0 AS DECIMAL(12,2)) AS shipping_amount,
         timestamp'2026-01-04 03:00:00' AS updated_at
  FROM late
),
same AS (       -- (e) existing orders re-sent with no change at all (same values, same updated_at)
  SELECT o.order_id, o.customer_id, o.order_date, o.order_status, o.shipping_address, o.payment_method, o.total_amount,
         o.discount_amount, o.tax_amount, o.shipping_amount, o.updated_at
  FROM orders o WHERE {U('o.order_id','same')} < 0.001
),
allr AS (
  SELECT * FROM upd UNION ALL SELECT * FROM newo2 UNION ALL SELECT * FROM late2 UNION ALL SELECT * FROM same
),
withdup AS (    -- (d) the source re-sent ~3% of rows
  SELECT * FROM allr UNION ALL SELECT * FROM allr WHERE {U('order_id','resend')} < 0.03
)
SELECT order_id, customer_id, order_date, order_status, shipping_address, payment_method, total_amount, discount_amount,
       tax_amount, shipping_amount, updated_at,
       timestamp_seconds(unix_timestamp(CASE WHEN updated_at < timestamp'2026-01-01 00:00:00' THEN timestamp'2026-01-03 12:00:00' ELSE updated_at END) + 120 + {H('order_id','ld',1800)}) AS loaded_at,
       concat('B', date_format(CASE WHEN updated_at < timestamp'2026-01-01 00:00:00' THEN timestamp'2026-01-03 12:00:00' ELSE updated_at END, 'yyyyMMdd')) AS batch_id
FROM withdup
""")
print("orders_incoming_batch:", spark.table("orders_incoming_batch").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14 · Declare informational primary keys and foreign keys
# MAGIC
# MAGIC Databricks stores these as metadata (useful for documentation, query planning hints and BI tools) but
# MAGIC does **not** enforce them. If your workspace does not support constraints this cell just reports and continues.

# COMMAND ----------

PKS = {
    "categories": "category_id", "warehouses": "warehouse_id", "products": "product_id", "customers": "customer_id",
    "orders": "order_id", "order_items": "order_item_id", "payments": "payment_id", "returns": "return_id",
    "inventory": "inventory_id", "customer_activity": "activity_id", "order_status_history": "history_id",
    "customer_changes": "change_id", "dim_date": "date_key",
}
FKS = [
    ("categories", "parent_category_id", "categories", "category_id"),
    ("products", "category_id", "categories", "category_id"),
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("payments", "order_id", "orders", "order_id"),
    ("returns", "order_item_id", "order_items", "order_item_id"),
    ("inventory", "product_id", "products", "product_id"),
    ("inventory", "warehouse_id", "warehouses", "warehouse_id"),
    ("customer_activity", "customer_id", "customers", "customer_id"),
    ("customer_activity", "product_id", "products", "product_id"),
    ("order_status_history", "order_id", "orders", "order_id"),
    ("customer_changes", "customer_id", "customers", "customer_id"),
]
ok = fail = 0
for t, c in PKS.items():
    try:
        run(f"ALTER TABLE {t} ADD CONSTRAINT pk_{t} PRIMARY KEY ({c})")
        ok += 1
    except Exception as e:
        fail += 1
        last_err = type(e).__name__ + ": " + (str(e).strip().splitlines() or [""])[0][:120]
for t, c, rt, rc in FKS:
    try:
        run(f"ALTER TABLE {t} ADD CONSTRAINT fk_{t}_{c} FOREIGN KEY ({c}) REFERENCES {rt}({rc})")
        ok += 1
    except Exception as e:
        fail += 1
        last_err = type(e).__name__ + ": " + (str(e).strip().splitlines() or [""])[0][:120]
print(f"Constraints declared: {ok}, skipped: {fail}" + (f" (e.g. {last_err})" if fail else ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15 · Clean up helper tables

# COMMAND ----------

for t in HELPER_TABLES:
    run(f"DROP TABLE IF EXISTS {t}")
print("Helper tables dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16 · Verification
# MAGIC
# MAGIC ### 16.1 Tables and row counts

# COMMAND ----------

MIN_ROWS = {"customers": 10000, "orders": 50000, "order_items": 100000, "products": 1000, "categories": 30,
            "payments": 50000, "returns": 5000, "inventory": 5000, "warehouses": 10, "customer_activity": 100000}
counts = []
for t in LAB_TABLES:
    n = spark.table(t).count()
    need = MIN_ROWS.get(t)
    counts.append((t, n, need, "OK" if (need is None or n >= need) else "TOO FEW"))
show(spark.createDataFrame(counts, "table_name string, row_count long, required_minimum int, check string"))
assert all(c[3] == "OK" for c in counts), "A table is smaller than the required minimum"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 16.2 Referential-integrity checks (every count below must be 0)

# COMMAND ----------

show(run("""
SELECT 'orders -> customers'       AS check_name, count(*) AS orphan_rows FROM orders o LEFT JOIN customers c ON c.customer_id = o.customer_id WHERE c.customer_id IS NULL
UNION ALL SELECT 'order_items -> orders',   count(*) FROM order_items i LEFT JOIN orders o ON o.order_id = i.order_id WHERE o.order_id IS NULL
UNION ALL SELECT 'order_items -> products', count(*) FROM order_items i LEFT JOIN products p ON p.product_id = i.product_id WHERE p.product_id IS NULL
UNION ALL SELECT 'payments -> orders',      count(*) FROM payments p LEFT JOIN orders o ON o.order_id = p.order_id WHERE o.order_id IS NULL
UNION ALL SELECT 'returns -> order_items',  count(*) FROM returns r LEFT JOIN order_items i ON i.order_item_id = r.order_item_id WHERE i.order_item_id IS NULL
UNION ALL SELECT 'products -> categories',  count(*) FROM products p LEFT JOIN categories c ON c.category_id = p.category_id WHERE c.category_id IS NULL
UNION ALL SELECT 'inventory -> products',   count(*) FROM inventory v LEFT JOIN products p ON p.product_id = v.product_id WHERE p.product_id IS NULL
UNION ALL SELECT 'inventory -> warehouses', count(*) FROM inventory v LEFT JOIN warehouses w ON w.warehouse_id = v.warehouse_id WHERE w.warehouse_id IS NULL
UNION ALL SELECT 'activity -> customers',   count(*) FROM customer_activity a LEFT JOIN customers c ON c.customer_id = a.customer_id WHERE a.customer_id IS NOT NULL AND c.customer_id IS NULL
UNION ALL SELECT 'status_history -> orders',count(*) FROM order_status_history h LEFT JOIN orders o ON o.order_id = h.order_id WHERE o.order_id IS NULL
UNION ALL SELECT 'customer_changes -> customers', count(*) FROM customer_changes c LEFT JOIN customers x ON x.customer_id = c.customer_id WHERE x.customer_id IS NULL
UNION ALL SELECT 'primary key duplicates (orders.order_id)', count(*) - count(DISTINCT order_id) FROM orders
"""))
bad = run("""
SELECT count(*) AS n FROM (
  SELECT 1 FROM orders o LEFT JOIN customers c ON c.customer_id = o.customer_id WHERE c.customer_id IS NULL
  UNION ALL SELECT 1 FROM order_items i LEFT JOIN orders o ON o.order_id = i.order_id WHERE o.order_id IS NULL
  UNION ALL SELECT 1 FROM order_items i LEFT JOIN products p ON p.product_id = i.product_id WHERE p.product_id IS NULL
  UNION ALL SELECT 1 FROM payments p LEFT JOIN orders o ON o.order_id = p.order_id WHERE o.order_id IS NULL
  UNION ALL SELECT 1 FROM returns r LEFT JOIN order_items i ON i.order_item_id = r.order_item_id WHERE i.order_item_id IS NULL
  UNION ALL SELECT 1 FROM products p LEFT JOIN categories c ON c.category_id = p.category_id WHERE c.category_id IS NULL
) t""").collect()[0]["n"]
assert bad == 0, "Referential integrity of the core relationships is broken"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 16.3 Sanity checks on the shape of the data

# COMMAND ----------

show(run("""
SELECT
  (SELECT min(order_date) FROM orders) AS first_order,
  (SELECT max(order_date) FROM orders) AS last_order,
  (SELECT count(DISTINCT customer_id) FROM orders) AS customers_with_orders,
  (SELECT count(*) FROM customers) AS customers_total,
  (SELECT round(avg(c), 2) FROM (SELECT count(*) c FROM order_items GROUP BY order_id)) AS avg_lines_per_order,
  (SELECT round(sum(total_amount) / 1e6, 1) FROM orders WHERE order_status = 'Delivered') AS delivered_revenue_million_inr
"""))
show(run("SELECT order_status, count(*) AS orders, round(100 * count(*) / sum(count(*)) OVER (), 1) AS pct FROM orders GROUP BY order_status ORDER BY orders DESC"))
show(run("SELECT year(order_date) AS yr, count(*) AS orders FROM orders GROUP BY 1 ORDER BY 1"))
print("Setup complete. Open 02_sql_practice_250_questions and start with Question 1.")
