# Spark Project: Refactoring Exercise

## Overview

This project contains a refactored Spark application that performs data enrichment. Follow the steps below to set up your home directory and run the application.

## Prerequisites

- **Spark Installation**: Ensure you have Spark installed on your system. Set the `SPARK_HOME` environment variable to the Spark installation directory.

## Instructions

1. **Clone the Repository**:
   git clone https://github.com/your-username/spark-refactoring.git
   cd spark-refactoring

2. **Run commands script**
    bash commands.sh

sales_table:
+---------+---------+--------+-------------------+----------+----+-----+---+----+
|InvoiceNo|StockCode|Quantity|        InvoiceDate|CustomerID|Year|Month|Day|Hour|
+---------+---------+--------+-------------------+----------+----+-----+---+----+
|   574683|    22930|       1|2011-11-06 12:50:00|     16372|2011|   11|  6|  12|
|   574683|    22931|       1|2011-11-06 12:50:00|     16372|2011|   11|  6|  12|
|   574671|    22938|       2|2011-11-06 12:23:00|     16744|2011|   11|  6|  12|
|   574683|    22940|       1|2011-11-06 12:50:00|     16372|2011|   11|  6|  12|
|   574684|    22941|       4|2011-11-06 12:56:00|     17581|2011|   11|  6|  12|
|   574678|    22942|       2|2011-11-06 12:36:00|     13365|2011|   11|  6|  12|
|   574677|    22943|       1|2011-11-06 12:32:00|     16445|2011|   11|  6|  12|
|   574678|    22945|      12|2011-11-06 12:36:00|     13365|2011|   11|  6|  12|
|   574669|    22947|       6|2011-11-06 12:16:00|     13727|2011|   11|  6|  12|
|   574676|    22948|       3|2011-11-06 12:32:00|     16243|2011|   11|  6|  12|
+---------+---------+--------+-------------------+----------+----+-----+---+----+

products_table:
+---------+--------------------+---------+
|StockCode|         Description|UnitPrice|
+---------+--------------------+---------+
|    10002|INFLATABLE POLITI...|     3.25|
|    10080|GROOVY CACTUS INF...|      4.0|
|    10080|               check|     0.12|
|    10120|        DOGGY RUBBER|     2.46|
|    10125|MINI FUNKY DESIGN...|     2.06|
|    10133|COLOURING PENCILS...|     1.86|
|    10133|             damaged|     4.08|
|    10135|COLOURING PENCILS...|     3.14|
|    11001|ASSTD DESIGN RACI...|     4.25|
|    15030|    FAN BLACK FRAME |     4.34|
+---------+--------------------+---------+

customers_table:
+----------+--------------+
|CustomerID|       Country|
+----------+--------------+
|     12346|United Kingdom|
|     12347|       Iceland|
|     12348|       Finland|
|     12349|         Italy|
|     12350|        Norway|
|     12352|        Norway|
|     12353|       Bahrain|
|     12354|         Spain|
|     12355|       Bahrain|
|     12356|      Portugal|
+----------+--------------+