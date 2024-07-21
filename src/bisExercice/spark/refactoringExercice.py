# Import necessary libraries
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession\
        .builder\
        .appName("SparkRefactoringExercise")\
        .getOrCreate()

    # Load CSV files into DataFrames
    path = "src/bisExercice/sources"
    orders_df = spark.read.csv(path + "/orders.csv", header=True, inferSchema=True)
    customers_df = spark.read.csv(path + "/customers.csv", header=True, inferSchema=True)
    products_df = spark.read.csv(path + "/products.csv", header=True, inferSchema=True)

    # Let's start by displaying a few rows from each DataFrame
    print("Orders Data:")
    orders_df.show(5)
    print("Customers Data:")
    customers_df.show(5)
    print("Products Data:")
    products_df.show(5)

    # Data cleansing
    # Drop missing data end duplicates


    spark.stop()