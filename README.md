# Spark Project: Refactoring Exercise

## Overview

This project contains a refactored Spark application that performs data ingestion and data analaytics. 
Follow the steps below to set up your home directory and run the application.

## Prerequisites

- **Spark Installation**: 

  - Ensure you have Spark installed on your system. 
  - Set the `SPARK_HOME` environment variable to the Spark installation directory.

## Instructions

1. **Clone the Repository**:
   - git clone git@github.com:a-agrz/bisExercice.git
   - cd bisExercice

2. **Run ingestion script**:
    - run ingestion script to launch the ingestion process and write to destination tables in output folder
    - bash runIngestionNotebook.sh
   
3. **Run analytics script**:
    - run analytics script to launch the analytics queries and write results to results folder
    - bash runAnalyticsNotebook.sh

## Code hierarchy
- src/bisExercice/spark/dataIngestion.py : pyspark notebook for ingesting data and insuring code quality, it contains answers to questions 1 and 2
- src/bisExercice/spark/dataAnalytics.py : pyspark notebook for data analytics, it contains answers to question 3
