# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC   
# MAGIC   # Transform and Load Data                                       

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Lab Overview
# MAGIC 
# MAGIC In this lab, you will learn how to prepare data and load that transformed data into Databricks Delta Tables. We will:
# MAGIC 
# MAGIC * Merge Data
# MAGIC * Join Data
# MAGIC * Change Data Types
# MAGIC * Remove Duplicate Values
# MAGIC * Resolve Data Discrepancies
# MAGIC * Create Views using Delta Tables

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Azure Databricks for Batch ETL & Data Engineers 
# MAGIC 
# MAGIC ![arch](https://kpistoropen.blob.core.windows.net/collateral/roadshow/azure_roadshow_de.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Libraries

# COMMAND ----------

from pyspark.sql.functions import col, when, count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Steps
# MAGIC ### 1. Read Data
# MAGIC Our campaign data came from two different sources, so we will need to merge them together as part of our data preparation steps. Let's merge the campaign_details_web and campaign_details_app CSVs.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's first connect to our data sources. There are three data files we will need to combine with merges and joins:
# MAGIC * Campaign Details - Web
# MAGIC * Campaign Details - App
# MAGIC * Campaign Results
# MAGIC 
# MAGIC Let's take a look at the files in the mount directory again.

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/data/final/")

# COMMAND ----------

# Create DataFrame for Web Campaign Details
webDetailsCSV = "/mnt/data/final/campaign_details_web.csv" # Create variable with link to our CSV file location
campaignDetailsWeb = (spark.read            # The DataFrameReader
   .option("header", "true")       # Use first line of all files as header
   .option("inferSchema", "true")  # Automatically infer data types
   .csv(webDetailsCSV)                   # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

# MAGIC %md
# MAGIC Use the code above to create DataFrames of the other two data sources. Use the variable names we have provided in the cell below.
# MAGIC 
# MAGIC Note: You can find the answers in cell 12 if you get stuck or prefer to run the code without writing it.

# COMMAND ----------

# Create DataFrames for the campaign_details_app and campaign_results CSV files. Use the code and directory information above as a guide.

# Create DataFrame for App Campaign Details
appDetailsCSV =  # Create variable with link to our CSV file location
campaignDetailsApp = 

# Create DataFrame for Campaign Results
resultsCSV = 
campaignResults = 

# COMMAND ----------

#### Answers ####


# Create DataFrame for App Campaign Details
appDetailsCSV = "/mnt/data/final/campaign_details_app.csv" # Create variable with link to our CSV file location
campaignDetailsApp = (spark.read            # The DataFrameReader
   .option("header", "true")       # Use first line of all files as header
   .option("inferSchema", "true")  # Automatically infer data types
   .csv(appDetailsCSV)                   # Creates a DataFrame from CSV after reading in the file
)

# Create DataFrame for Campaign Results
resultsCSV = "/mnt/data/final/campaign_results_aug.csv" # Create variable with link to our CSV file location
campaignResults = (spark.read            # The DataFrameReader
   .option("header", "true")       # Use first line of all files as header
   .option("inferSchema", "true")  # Automatically infer data types
   .csv(resultsCSV)                   # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.1 Review Data
# MAGIC Now that all three DataFrames are available, let's make sure we have the same number and types of columns before we attempt to join the data. We can do this by looking at the schemas. 
# MAGIC 
# MAGIC **Reminder:** We need to MERGE campaignDetailsWeb and campaignDetailsAPP. Then we will JOIN the resulting DataFrame to CampaignResults. Let's review the campaign details DataFrames first.

# COMMAND ----------

campaignDetailsWeb.printSchema()

# COMMAND ----------

campaignDetailsApp.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC The dm_campaign field is a string in one of our DataFrames and an integer in the other. We will cast it as a string before we merge our DataFrames.

# COMMAND ----------

# Cast dm_campaign as string

campaignDetailsWeb = campaignDetailsWeb.withColumn( "dm_campaign" # add a column named dm_campaign (this will override our old one)
              , col("dm_campaign") #the column will be populated with data from the original dm_campaign field
              .cast("string")) # the new column will be a string

campaignDetailsWeb.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Combine DataFrames
# MAGIC #### 2.1 Merge Data
# MAGIC Now let's merge our DataFrames.

# COMMAND ----------

# Add Code to Merge campaign_details_web and campaign_details_app

campaignDetailsAll = campaignDetailsWeb.union(campaignDetailsApp) #merge the two DataFrames

display(campaignDetailsAll.sort("customer")) # display the results, sorted by customer

# COMMAND ----------

# MAGIC %md
# MAGIC If we want to confirm our data merged without dropping any rows, we can check the counts.

# COMMAND ----------

campaignDetailsWeb.count() + campaignDetailsApp.count(), campaignDetailsAll.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.2 Join Data
# MAGIC In addition to merging our separate campaign files, we also need to join the campaign results data to get a unified view for our data analysts and data scientists.

# COMMAND ----------

# MAGIC %md
# MAGIC Before we can merge our data, we need to know which column to use as our key. Let's check the schema of our campaignResults DataFrame

# COMMAND ----------

campaignResults.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC It looks like a customer_id is present in both DataFrames, though it is defined as customer in one of them. Let's try joining by those columns now.

# COMMAND ----------

campaignAll = campaignDetailsAll.join(campaignResults, campaignDetailsAll.customer == campaignResults.customer_id, how = "left").drop(campaignResults.customer_id)

display(campaignAll)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take a look at our schema.

# COMMAND ----------

campaignAll.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC We need to change the two doubles to decimals for aggregation later by our analysts.

# COMMAND ----------

# Use cell 17 as your guide

campaignAll = campaignAll.withColumn(#code here
)

campaignAll = campaignAll.withColumn(#code here
)


# COMMAND ----------

#### Answers ####




campaignAll = campaignAll.withColumn( "previous_spend" # add a column named dm_campaign (this will override our old one)
              , col("previous_spend") #the column will be populated with data from the original dm_campaign field
              .cast("decimal")) # the new column will be a decimal

campaignAll = campaignAll.withColumn( "spend" # add a column named dm_campaign (this will override our old one)
              , col("spend") #the column will be populated with data from the original dm_campaign field
              .cast("decimal")) # the new column will be a decimal


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Cleanse Data
# MAGIC With the flexibility of Python (or any other supported language) Databricks provides the capacity to perform all of your data transformations. In this lab, we will focus on a few simple examples, but the possibilities are endless.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.1 Remove Duplicates
# MAGIC Before doing any analysis on data, it's always a good idea to ensure that we don't have any duplicate records. Let's see if we have any duplicate records.

# COMMAND ----------

# Count of all rows in DF minus count of DF rows with duplicates removed.
campaignAll.count() - campaignAll.dropDuplicates().count()

# COMMAND ----------

# MAGIC %md
# MAGIC It looks like we have duplicate records. Since we do have a few duplicate rows, let's remove them. 

# COMMAND ----------

campaignAll = campaignAll.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.2 Resolve Data Discrepancies
# MAGIC Often when data is joined, there can be issues with consistency between data sources. Let's take a look at our data and see if we can find any problems.

# COMMAND ----------

display(campaignAll)

# COMMAND ----------

# MAGIC %md
# MAGIC It looks like Suburbs and Suburban are both used to specify the same location. We need to change all instances of Suburbs to Suburban.

# COMMAND ----------

campaignAll = campaignAll.withColumn("customer_location", when(col("customer_location") == "Suburbs", "Suburban").otherwise(col("customer_location")))
display(campaignAll)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3 Drop Fields
# MAGIC Another common data preparation step is to remove any fields that are not needed for analysis or modeling. Let's explore that dm_campaign column a little more. It looked like it had a lot of nulls.

# COMMAND ----------

campaignAll.filter(col("dm_campaign").isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC We need to know how many total columns are in the DataFrame to know how significant that is. 

# COMMAND ----------

campaignAll.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's calculate the percentage of records that are null

# COMMAND ----------

campaignAll.filter(col("dm_campaign").isNull()).count()/campaignDetailsAll.count()

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that the dm_campaign field has very little data. After speaking with our marketing team, we learned that this field was used in very early tests but later dropped. They have agreed that it is not needed in any final views. Let's remove it now.

# COMMAND ----------

campaignAll = campaignAll.drop('dm_campaign')

display(campaignAll)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Delta Lake for Efficient Upserts into your Data Lake
# MAGIC 
# MAGIC ![arch](https://databricks.com/wp-content/uploads/2019/03/UpsertsBlog.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Create Views using Delta Tables
# MAGIC Now that we have prepared the data, we need to make it accessible to our analysts and data scientists. We will load the data into a Databricks Delta Table for easy access across Databricks. Because Delta tables are accessible to everyone, only the instructor will run the cell to create the Delta table. Afterward, the whole class can use it for the remainder of the labs.
# MAGIC 
# MAGIC Note: Delta tables have many other benefits. We will continue to use this table throughout the course and learn more about them.

# COMMAND ----------

#ONLY THE INSTRUCTOR NEEDS TO RUN THIS CELL
spark.sql("""
  DROP TABLE IF EXISTS campaign_all_delta
""")

campaignAll.write.format("delta").mode("overwrite").save("/delta/analysts")
spark.sql("CREATE TABLE campaign_all_delta USING DELTA LOCATION '/delta/analysts/'")

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have loaded the data into the table, we can perform regular SQL queries on it.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM campaign_all_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM campaign_all_delta

# COMMAND ----------

# MAGIC %md
# MAGIC In addition to the queries we can run, we can also create DataFrames and explore the data in Databricks. We will look at this more in the next lab.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resources
# MAGIC https://docs.databricks.com/delta/index.html
