# Import necessary libraries
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession \
        .builder \
        .appName("DataIngestion") \
        .getOrCreate()

    # Load CSV files into DataFrames
    path = "src/bisExercice/sources"
    orders_df = spark.read.csv(path + "/orders.csv", header=True, inferSchema=True)
    customers_df = spark.read.csv(path + "/customers.csv", header=True, inferSchema=True)
    products_df = spark.read.csv(path + "/products.csv", header=True, inferSchema=True)

    # Let's start by displaying a few rows from each DataFrame
    print("Orders Data:")
    orders_df.show(5)
    print("Orders Data count : ")
    orders_df.count()
    print("Customers Data:")
    customers_df.show(5)
    print("Customers Data count : ")
    customers_df.count()
    print("Products Data:")
    products_df.show(5)
    print("Products Data count : ")
    products_df.count()

    # due to error in timestamp handling we need to add this line if working with spark version >3.0
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    # ** Ordres table **

    # Data Validation
    orders_df = orders_df.filter("InvoiceNo IS NOT NULL AND StockCode IS NOT NULL AND Quantity >= 0")
    orders_df = orders_df.withColumn("InvoiceDate", to_timestamp("InvoiceDate", "MM/dd/yyyy HH:mm"))
    # Data Cleaning
    orders_df = orders_df.dropna(subset=["CustomerID"])

    # ** Customers Table **

    # Data cleaning and Validation
    customers_df = customers_df.filter("CustomerID IS NOT NULL AND Country IS NOT NULL")
    customers_df = customers_df.dropna(subset=["CustomerID"])

    # ** Products Table **

    # Data Cleaning and Validation
    products_df = products_df.filter("StockCode IS NOT NULL AND Description IS NOT NULL AND UnitPrice >= 0")
    products_df = products_df.dropna(subset=["StockCode", "Description"])

    print("Orders Data:")
    orders_df.show(5)
    print("Orders Data count : ")
    orders_df.count()
    print("Customers Data:")
    customers_df.show(5)
    print("Customers Data count : ")
    customers_df.count()
    print("Products Data:")
    products_df.show(5)
    print("Products Data count : ")
    products_df.count()

    # **** Data Ingestion  ****
    output_Path = "src/bisExercice/output"

    # Define schema for Sales (Fact) table
    sales_schema = StructType([
        StructField("InvoiceNo", StringType(), nullable=False),
        StructField("StockCode", StringType(), nullable=False),
        StructField("Quantity", IntegerType(), nullable=False),
        StructField("InvoiceDate", TimestampType(), nullable=True),
        StructField("CustomerID", IntegerType(), nullable=True),
    ])

    # Create Sales DataFrame with defined schema
    sales_df = spark.createDataFrame(orders_df.rdd, schema=sales_schema)

    # Create additional columns for year, month, day, and hour for partitioning
    sales_df = sales_df.withColumn("Year", F.year("InvoiceDate"))
    sales_df = sales_df.withColumn("Month", F.month("InvoiceDate"))
    sales_df = sales_df.withColumn("Day", F.dayofmonth("InvoiceDate"))
    sales_df = sales_df.withColumn("Hour", F.hour("InvoiceDate"))

    # Write Sales table to a target location (Parquet format)
    sales_df.write\
        .mode("overwrite")\
        .partitionBy("Year", "Month", "Day", "Hour")\
        .parquet(output_Path + "/sales_table")

    # Define schema for Customers (Dimension) table
    customers_schema = StructType([
        StructField("CustomerID", IntegerType(), nullable=False),
        StructField("Country", StringType(), nullable=True)  # Assuming nullable for Country
    ])

    # Create Customers DataFrame with defined schema
    customers_df = spark.createDataFrame(customers_df.rdd, schema=customers_schema)

    # Write Customers table to a target location
    customers_df.write\
        .mode("overwrite")\
        .parquet(output_Path + "/customers_table")

    # Define schema for Products (Dimension) table
    products_schema = StructType([
        StructField("StockCode", StringType(), nullable=False),
        StructField("Description", StringType(), nullable=True),  # Assuming nullable for Description
        StructField("UnitPrice", DoubleType(), nullable=False)  # Assuming nullable for UnitPrice
    ])

    # Create Products DataFrame with defined schema
    products_df = spark.createDataFrame(products_df.rdd, schema=products_schema)

    # Write Products table to a target location
    products_df.write\
        .mode("overwrite")\
        .parquet(output_Path + "/products_table")

    print("*********** fin *************")
    spark.stop()
