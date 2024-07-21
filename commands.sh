#!/bin/bash

# Set your Spark home directory
SPARK_HOME="/opt/homebrew/opt/apache-spark"

# Submit the Spark application
$SPARK_HOME/bin/spark-submit \
  --master "local[4]" \
  src/bisExercice/spark/refactoringExercice.py