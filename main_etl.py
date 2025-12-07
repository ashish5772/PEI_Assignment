from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

# ---------- RAW INGESTION ----------
df_cust = (
    spark.read.format("excel")
    .option("headerRows", "1")
    .load("/Workspace/Users/ashish577@gmail.com/Customer.xlsx")
)

df_orders = spark.read.option("multiLine", "true").json("/Workspace/Users/ashish577@gmail.com/Orders.json")

df_products = spark.read.option("header", "true").csv("/Workspace/Users/ashish577@gmail.com/Products.csv")

def process_cols(cols):
    return [i.lower().replace(" ", "_").replace("-", "_") for i in cols]

df_products = df_products.toDF(*process_cols(df_products.columns))
df_orders = df_orders.toDF(*process_cols(df_orders.columns))
df_cust = df_cust.toDF(*process_cols(df_cust.columns))

df_products.write.mode("overwrite").saveAsTable("products_raw")
df_orders.write.mode("overwrite").saveAsTable("orders_raw")
df_cust.write.mode("overwrite").saveAsTable("customers_raw")

# ---------- DATA VALIDATION & ENRICHMENT ----------
def validate_column(df, colm, regex_rep, regex_match):
    return (
        df.withColumn(colm, F.regexp_replace(colm, regex_rep, ""))
        .withColumn(colm, F.when(F.col(colm).rlike(regex_match), F.col(colm)).otherwise(None))
    )

customers_raw_df = spark.table("customers_raw")

# Cleaning regex
regexp_phone_rep = r"[^0-9+()\- ]"
regexp_email_rep = r"[^a-zA-Z0-9._@-]"
regexp_name_rep = r"[^a-zA-Z0-9 `]"

# Validation regex
regexp_phone = r"^\+?[0-9\s\-()]{7,20}$"
regexp_email = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+"

# Trim whitespace
for c in customers_raw_df.columns:
    customers_raw_df = customers_raw_df.withColumn(c, F.trim(c))

customers_raw_df = validate_column(customers_raw_df, "phone", regexp_phone_rep, regexp_phone)
customers_raw_df = validate_column(customers_raw_df, "email", regexp_email_rep, regexp_email)
customers_raw_df = validate_column(customers_raw_df, "customer_name", regexp_name_rep, ".*")

customers_raw_df.write.mode("overwrite").saveAsTable("customer_enriched")


products_raw_df = spark.table("products_raw")

regexp_prod_name = r"[^a-zA-Z0-9 `]"
regexp_state = r"[^a-zA-Z]"

for c in products_raw_df.columns:
    products_raw_df = products_raw_df.withColumn(c, F.trim(c))

products_raw_df = validate_column(products_raw_df, "product_name", regexp_prod_name, ".*")
products_raw_df = validate_column(products_raw_df, "state", regexp_state, ".*")

products_raw_df.write.mode("overwrite").saveAsTable("product_enriched")

# ---------- FINAL ORDER ENRICHMENT ----------
df_orders_raw = spark.table("orders_raw")
df_customers = spark.table("customer_enriched")
df_products = spark.table("product_enriched")

df_orders_raw = df_orders_raw.withColumn("profit", F.round("profit", 2))
df_orders_raw = df_orders_raw.withColumn("order_date", F.to_date("order_date", "d/M/yyyy"))

df_result = (
    df_orders_raw.alias("o")
    .join(df_customers.alias("c"), "customer_id", "left")
    .select("o.*", "c.customer_name", "c.country")
)

df_result = (
    df_result.alias("o")
    .join(df_products.alias("p"), "product_id", "left")
    .select("o.*", "p.category", "p.sub_category")
)

df_result.write.mode("overwrite").saveAsTable("order_enriched")
