# Create Aggregate Table
spark.sql("""
CREATE OR REPLACE TABLE order_agg AS
SELECT 
    ROUND(SUM(NVL(profit,0)),2) AS agg_profit,
    YEAR(order_date) AS year,
    category,
    sub_category,
    customer_id
FROM order_enriched
GROUP BY year, category, sub_category, customer_id
""")

# ---- FINAL REPORTS ----

# Profit by Year
profit_by_year = spark.sql("""
SELECT year, SUM(NVL(agg_profit,0)) AS total_profit 
FROM order_agg 
GROUP BY year 
ORDER BY year
""")
profit_by_year.show()

# Profit by Year + Category
profit_by_year_category = spark.sql("""
SELECT year, category, SUM(NVL(agg_profit,0)) AS total_profit 
FROM order_agg 
GROUP BY year, category 
ORDER BY year, category
""")
profit_by_year_category.show()

# Profit by Customer
profit_by_customer = spark.sql("""
SELECT customer_id, SUM(NVL(agg_profit,0)) AS total_profit 
FROM order_agg 
GROUP BY customer_id 
ORDER BY total_profit DESC
""")
profit_by_customer.show()

# Profit by Customer + Year
profit_by_customer_year = spark.sql("""
SELECT customer_id, year, SUM(NVL(agg_profit,0)) AS total_profit 
FROM order_agg 
GROUP BY customer_id, year 
ORDER BY customer_id, year
""")
profit_by_customer_year.show()
