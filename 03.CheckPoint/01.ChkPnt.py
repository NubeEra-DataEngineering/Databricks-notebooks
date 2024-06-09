# Databricks notebook source

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StringType, TimestampType

# Initialize Spark session
spark = SparkSession.builder.appName("CheckpointingDemo").getOrCreate()

# Define the schema for the events data
schema = StructType() \
    .add("date", TimestampType()) \
    .add("event_type", StringType()) \
    .add("event_message", StringType())

# Load sample dataset from databricks-datasets
input_path = "/databricks-datasets/structured-streaming/events/"

# Read the input data stream
input_stream = spark.readStream \
    .schema(schema) \
    .json(input_path)

# Apply some transformation
transformed_stream = input_stream \
    .withColumn("event_date", expr("date_trunc('day', date)")) \
    .groupBy("event_date", "event_type") \
    .count() \
    .orderBy("event_date", "event_type")

# Define the output path and checkpoint location
output_path = "/tmp/output/checkpointing_demo"
checkpoint_location = "/tmp/checkpoints/checkpointing_demo"

# Write the transformed stream to the output with checkpointing
query = transformed_stream.writeStream \
    .outputMode("complete") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

# Wait for the termination of the query
query.awaitTermination()

# COMMAND ----------

# Reading the output data to verify
output_df = spark.read.parquet(output_path)
display(output_df)

# COMMAND ----------

# Stopping the Spark session
spark.stop()
