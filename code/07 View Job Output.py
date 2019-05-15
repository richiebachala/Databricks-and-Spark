# Databricks notebook source
# MAGIC %md
# MAGIC # View Job Output

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In this lab, we will:
# MAGIC * Read the File Generated from the Job Run
# MAGIC * View the DataFrame

# COMMAND ----------

yourCampaignPivot = spark.read.format("csv").load('/mnt/data/final/campaign_pivot_' + 'dj' + '.csv')

# COMMAND ----------

display(yourCampaignPivot)
