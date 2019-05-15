# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC   
# MAGIC   # Explore Data                                      

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Lab Overview
# MAGIC Working as marketing analysts, we will explore our data and look for answers to a few questions:
# MAGIC 1. How does customer spend compare across channels?
# MAGIC 2. When looking at discount amounts, do we see a dip in spend for higher discount amounts?
# MAGIC 3. Can we identify any instance in which a lower discount amount leads to higher spend or more conversions?
# MAGIC 
# MAGIC In this lab, we will:
# MAGIC * Read a Databricks Delta Table
# MAGIC * Aggregate Data
# MAGIC * Quickly Visualize Data

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Azure Databricks for Serverless SQL & BI Queries for Data at Scale 
# MAGIC 
# MAGIC ![arch](https://kpistoropen.blob.core.windows.net/collateral/roadshow/azure_roadshow_bi.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Steps
# MAGIC ### 1. Read Data from Delta Tables
# MAGIC Let's take a look at the data in our Marketing Analysts Delta Table again. We will create a DataFrame to explore the data.

# COMMAND ----------

campaignAllDelta = spark.read.format("delta").load("/delta/analysts/")

display(campaignAllDelta)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Aggregate Data
# MAGIC #### 2.1 Aggregate by Spend
# MAGIC 
# MAGIC _How does customer spend compare across channels?_
# MAGIC 
# MAGIC This is the first time the marketing analysts have seen the results of both campaigns together all in one place. Let's aggregate the spend by channel and see how the spend compares across the channels.

# COMMAND ----------

from pyspark.sql.functions import sum, col, avg

spendByChannel = campaignAllDelta.groupBy("channel").agg(sum(col("spend")).alias("spend")).select("channel","spend").orderBy("spend")
display(spendByChannel)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC _When looking at discount amounts, do we see a dip in spend for higher discount amounts?_

# COMMAND ----------

spendByCampaign = campaignAllDelta.groupBy("campaign").agg(sum(col("spend")).alias("spend")).select("campaign","spend").orderBy("spend")
display(spendByCampaign)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, you're going to use the code above to aggregate by another field, such as customer location in the cell below. You could even look at previous spend. Take some time to explore the data.

# COMMAND ----------

# Use the code above to aggregate spend by another field 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.2 Use Visualization to Explore
# MAGIC 
# MAGIC _Can we identify any instance in which a lower discount amount leads to higher spend or more conversions?_
# MAGIC 
# MAGIC In this section, we are going to see how we can use the one-click visualization capabilities to explore the data in Databricks without needing to code. We will display the full table, visualize it, then change fields and chart types to determine whether there are any slices of the population where a discount amount of less than 15 led to more conversions or spend.

# COMMAND ----------

# Run this code and explore it using the visualization tool.
display(campaignAllDelta)
s