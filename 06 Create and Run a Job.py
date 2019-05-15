# Databricks notebook source
# MAGIC %md 
# MAGIC # Create and Run a Job

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Lab Overview
# MAGIC 
# MAGIC In this lab, we will:
# MAGIC * View Code for Constructing a Simple BI Report
# MAGIC * Create a Job to Run this Notebook
# MAGIC * Run the Job

# COMMAND ----------

# MAGIC %md
# MAGIC ## Steps
# MAGIC ### 1. Load Required Libraries

# COMMAND ----------

from pyspark.sql.functions import col, sum, avg

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read Data from Delta Tables
# MAGIC 
# MAGIC Use the Marketing Analysts Delta Table.

# COMMAND ----------

campaignAllDelta = spark.read.format("delta").load("/delta/analysts/")

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Aggregate and Summarize
# MAGIC 
# MAGIC Our business analysts want a simple report that provides the average spend per conversion for each campaign. 
# MAGIC 
# MAGIC They want this metric freshly updated each day in file they can easily access. 

# COMMAND ----------

campaignPivot = (campaignAllDelta
                 .filter(col("conversion")==1)
                 .groupBy("campaign")
                 .agg(avg(col("spend")).alias("average_spend"))
                )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Write to File

# COMMAND ----------

campaignPivot.write.csv('/mnt/data/final/campaign_pivot_' + 'dj' + '.csv')
