# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC   
# MAGIC   # Connect to Streaming Data                                       

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Lab Overview
# MAGIC ### In this section, you will learn to:
# MAGIC * Connect to a Streaming Data Source
# MAGIC * View and Interact with Streaming Data
# MAGIC * Insert Streaming Data into Delta Table

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## What is Structured Streaming?
# MAGIC 
# MAGIC <div style="width: 100%">
# MAGIC   <div style="margin: auto; width: 800px">
# MAGIC     <img src="http://spark.apache.org/docs/latest/img/structured-streaming-stream-as-a-table.png"/>
# MAGIC   </div>
# MAGIC </div>
# MAGIC 
# MAGIC Data is appended to the Input Table every _trigger interval_. For instance, if the trigger interval is 1 second, then new data is appended to the Input Table every seconds. (The trigger interval is analogous to the _batch interval_ in the legacy RDD-based Streaming API.) 

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Azure Databricks for Streaming Analytics, Alerts, ETL & Data Engineers 
# MAGIC 
# MAGIC ![arch](https://kpistoropen.blob.core.windows.net/collateral/roadshow/azure_roadshow_sde.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Steps
# MAGIC ### 1. Connect to Streaming Data

# COMMAND ----------

from pyspark.sql.types import * # Required for StructField, StringType, IntegerType, etc.

streamSchema = StructType([ # Create a variable that contains our defined schema
  StructField("customer", IntegerType(), True),
  StructField("last_purchase", StringType(), True), 
  StructField("previous_spend", DecimalType(), True),
  StructField("mens", IntegerType(), True),
  StructField("womens", IntegerType(), True),
  StructField("customer_location", StringType(), True),
  StructField("newbie", IntegerType(), True),
  StructField("channel", StringType(), True),
  StructField("campaign", IntegerType(), True),
  StructField("conversion", IntegerType(), True),
  StructField("spend", DecimalType(), True)
 ])

# COMMAND ----------

#streaming DataFrame reader for data on Azure Storage

streamingDF = spark.readStream \
    .schema(streamSchema) \
    .option("maxFilesPerTrigger", 1) \
    .csv("dbfs:/mnt/data/final/stream/part*")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. View Streaming Data 
# MAGIC 
# MAGIC If you run the following cell, you'll get a continuously updating display of the number of records read from the stream so far. Note that we're just calling `display()` on our DataFrame, _exactly_ as if it were a DataFrame reading from a static data source.
# MAGIC 
# MAGIC To stop the continuous update, just cancel the query.

# COMMAND ----------

display(streamingDF)

# COMMAND ----------

# MAGIC %md ### 3. Transform Streaming DataFrame 
# MAGIC We can use normal DataFrame transformations on our streaming DataFrame. For example, let's group the conversions by campaign type.

# COMMAND ----------

from pyspark.sql.functions import *
discountsDisplayed = streamingDF.groupBy("campaign").agg(count(col("conversion")==1).alias("spend")).orderBy(desc("spend"))

# COMMAND ----------

display(discountsDisplayed)

# COMMAND ----------

# MAGIC %md ### 4. Write Into Delta Table
# MAGIC It is possible to write our streaming data into a Delta Table. This would allow the analysts to have a live updated view of the campaign results as users are presented offers. 

# COMMAND ----------

# Only the instructor should run this code!!
streamingDF.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/delta/analysts/check/new").table("campaign_all_delta_stream")

# COMMAND ----------

streamDelta = spark.readStream.format("delta").table("campaign_all_delta_stream")

display(streamDelta)

# COMMAND ----------

updatedDelta = spark.read.format("delta").table("campaign_all_delta_stream")

updatedDelta.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Turn off Your Streams!

# COMMAND ----------

for streamingQuery in spark.streams.active:
  streamingQuery.stop()

# COMMAND ----------

