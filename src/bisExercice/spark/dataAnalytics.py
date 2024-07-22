from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from pyspark.sql.functions import max, min

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession \
        .builder \
        .appName("DataAnalytics") \
        .getOrCreate()

    # Load ingested parquet files into DataFrames
    path = "src/bisExercice/output"
    sales_df = spark.read.parquet(path + "/sales_table")
    customers_df = spark.read.parquet(path + "/customers_table")
    products_df = spark.read.parquet(path + "/products_table")
    rslt_Path = "src/bisExercice/results"

    ######### Top 10 Countries with the Most Number of Customers: #########

    # Group by country and count the number of customers
    country_counts = customers_df.groupBy("Country").count()
    # Get the top 10 countries with the most customers
    top_10_countries = country_counts.orderBy("count", ascending=False).limit(10)
    top_10_countries.show()
    top_10_countries.write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(rslt_Path + "/top_10_countries")

    ######### Revenue Distribution by Country: #########

    # Join the tables on the common columns "StockCode" and "CustomerID"
    joined_df = sales_df\
        .join(products_df, on="StockCode", how="inner")\
        .join(customers_df, on="CustomerID", how="inner")\
        .drop('CustomerID','InvoiceNo','Description')

    # Calculate revenue (total sales amount) per country
    revenue_by_country = joined_df\
        .groupBy("Country")\
        .agg(sum(col("Quantity") * col("UnitPrice"))\
        .alias("Revenue"))
    revenue_by_country.show()
    revenue_by_country.write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(rslt_Path + "/revenue_by_country")

    ######### Relationship Between Average Unit Price and Sales Volume: #########

    # Calculate average unit price per product
    avg_unit_price = products_df\
        .groupBy("StockCode")\
        .avg("UnitPrice")\
        .withColumnRenamed("avg(UnitPrice)","AvgUnitPrice")
    # Join with sales data to get sales volume
    sales_volume = sales_df\
        .groupBy("StockCode")\
        .sum("Quantity")\
        .withColumnRenamed("sum(Quantity)", "SalesVolume")
    # Join both DataFrames
    unit_price_sales_volume = avg_unit_price.join(sales_volume, "StockCode")

    unit_price_sales_volume.show()
    unit_price_sales_volume.write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(rslt_Path + "/unit_price_sales_volume")

    ######### Top 3 Products with Maximum Unit Price Drop in the Last Month: #########

    # Filter for the last month
    joined_df_lastMonth = joined_df.filter("Year >= '2011' AND Month >= '12'")

    # Calculate the unit price change
    window_spec = Window.partitionBy("StockCode").orderBy("InvoiceDate")
    unit_price_change = joined_df_lastMonth.withColumn("PrevUnitPrice", lag("UnitPrice").over(window_spec))
    unit_price_change = unit_price_change.withColumn("UnitPriceChange", col("UnitPrice") - col("PrevUnitPrice"))

    unit_price_change_filtered = unit_price_change\
        .select('StockCode',  'UnitPriceChange')\
        .filter("UnitPriceChange IS NOT NULL")
    # Get the top 3 products with maximum unit price drop
    top_3_price_drop=unit_price_change_filtered\
        .groupBy('StockCode')\
        .agg(min('UnitPriceChange'))\
        .orderBy('min(UnitPriceChange)',ascending=False).limit(3)
    top_3_price_drop.show()
    top_3_price_drop.write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(rslt_Path + "/top_3_price_drop")

    print("*********** fin *************")
    # Stop Spark session
    spark.stop()