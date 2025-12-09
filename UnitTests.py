import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from main_etl import process_cols, validate_column


#Spark Fixture

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("ETL_Pipeline_Test") \
        .getOrCreate()


#Test Data Fixtures
@pytest.fixture
def sample_customer_df(spark):
    return spark.createDataFrame(
        [
            (" John  ", "mary@gmail.com", "+91 999-555-222"),
            ("Bob##", "invalid_email@", "abc123"),
            ("Alic3", "alice@@gmail.com", "(123)444")
        ],
        ["customer_name", "email", "phone"]
    )


@pytest.fixture
def sample_order_df(spark):
    return spark.createDataFrame(
        [
            ("21/8/2016", 99.456),
            ("03/10/2014", 10.899),
        ],
        ["order_date", "profit"]
    )

@pytest.fixture
def customer_raw_df(spark):
    return spark.read \
        .format("excel") \
        .option("headerRows", 1) \
        .load("/Workspace/Users/Customer.xlsx")


@pytest.fixture
def product_raw_df(spark):
    return spark.read.option("header", True).csv("/Workspace/Users/Products.csv")


@pytest.fixture
def orders_raw_df(spark):
    return spark.read.option("multiLine", True).json("/Workspace/Users/Orders.json")



#Unit Tests
def test_raw_customer_columns(customer_raw_df):
    expected = {"Customer ID", "Customer Name", "email", "phone","address","Segment", "Country","City","State","Postal Code","Region"}
    assert expected.issubset(set(customer_raw_df.columns)), \
        f"Missing original expected customer columns. Found: {customer_raw_df.columns}"
        
def test_raw_product_columns(product_raw_df):
    expected = {"Product ID","Category","Sub-Category", "Product Name", "State", "Price per product"}
    assert expected.issubset(set(product_raw_df.columns)), \
        f"Missing expected product columns. Found: {product_raw_df.columns}"


def test_raw_order_columns(orders_raw_df):
    expected = {"Row ID","Order ID", "Order Date","Ship Date","Ship Mode", "Customer ID", "Product ID", "Quantity", "Price", "Discount", "Profit"}
    assert expected.issubset(set(orders_raw_df.columns)), \
        f"Missing expected order columns. Found: {orders_raw_df.columns}"
        
        
def test_process_cols():
    cols = ["Customer Name", "Order-Date", "PHONE Number"]
    expected = ["customer_name", "order_date", "phone_number"]
    assert process_cols(cols) == expected


def test_validate_column_phone(sample_customer_df):
    regexp_phone_rep = r'[^0-9+()\- ]'
    regexp_phone = r'^\+?[0-9\s\-()]{7,20}$'

    result = validate_column(sample_customer_df, "phone", regexp_phone_rep, regexp_phone)

    cleaned = result.select("phone").collect()

    assert cleaned[0]["phone"] == "+91 999-555-222"     # valid stays
    assert cleaned[1]["phone"] is None                  # invalid becomes null
    assert cleaned[2]["phone"] == "(123)444"            # valid short format


def test_validate_column_email(sample_customer_df):
    regexp_email_rep = r'[^a-zA-Z0-9._@+-]'
    regexp_email = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+'

    result = validate_column(sample_customer_df, "email", regexp_email_rep, regexp_email)

    values = [row["email"] for row in result.collect()]
    
    assert values == ["mary@gmail.com",
        None,                   # invalid format
        "alice@@gmail.com"      # allowed by your regex pattern
    ]


def test_validate_column_name(sample_customer_df):
    regexp_name_rep = r'[^a-zA-Z 0-9`]'

    result = validate_column(sample_customer_df, "customer_name", regexp_name_rep, ".*")

    names = [row["customer_name"] for row in result.collect()]

    assert names == [" John  ", "Bob", "Alic3"]


def test_profit_rounding(sample_order_df):
    df = sample_order_df.withColumn("profit", F.round("profit", 2))

    rows = df.collect()
    assert rows[0]["profit"] == 99.46
    assert rows[1]["profit"] == 10.9


def test_date_conversion(sample_order_df):
    df = sample_order_df.withColumn(
        "order_date", F.to_date("order_date", "d/M/yyyy")
    )

    rows = df.collect()

    assert str(rows[0]["order_date"]) == "2016-08-21"
    assert str(rows[1]["order_date"]) == "2014-10-03"
    

def test_no_nulls_in_id_columns(orders_raw_df, customer_raw_df, product_raw_df):

    id_rules = {
        "Orders": (orders_raw_df, ["Order ID", "Customer ID", "Product ID"]),
        "Customers": (customer_raw_df, ["Customer ID"]),
        "Products": (product_raw_df, ["Product ID"]),
    }

    failures = {}

    for name, (df, required_cols) in id_rules.items():
        for col in required_cols:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                failures[f"{name}.{col}"] = null_count

    assert failures == {}, f"Nulls found in ID fields: {failures}"
    
    
def test_null_threshold(orders_raw_df, customer_raw_df, product_raw_df):
    #Checks if there are more than 5% nulls in the data, if so, it fails
    threshold = 0.05  # 5%

    datasets = {
        "Orders": orders_raw_df,
        "Customers": customer_raw_df,
        "Products": product_raw_df,
    }

    violations = {}

    for name, df in datasets.items():
        total = df.count()
        if total == 0:
            continue  

        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_ratio = null_count / total

            if null_ratio > threshold:
                violations[f"{name}.{col}"] = f"{null_ratio}"

    assert violations == {}, f"Columns exceeding 5% nulls: {violations}"
    
