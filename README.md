# Databricks-and-Spark
 Feature Engineering, Spark ML Random Forest Model, Log MLFlow, Streaming Data Source

# Databricks notebook source
%md

  
  #  Create DataFrames                               

# COMMAND ----------

%md 
## Lab Overview

As data engineers, we need to make data available to our marketing analysts and data scientists for reporting and modeling. The first step in that process, is to read in data and define schemas.

In this section, you will learn how to:

1. Read Mounted Data
2. Create Dataframes
3. View, Infer, and Define Schemas   

# COMMAND ----------

%md 
####Azure Databricks is a Unified Analytics Platform for Data Engineers, Data Scientist, and Analysis  

![arch](https://kpistoropen.blob.core.windows.net/collateral/roadshow/azure_roadshow_1.png)

# COMMAND ----------

%md
## Steps
### 1. Read Mounted Data
For our labs today, we have already mounted the data that we will be accessing. Here is the basic code you would use to mount a data source from Azure blob storage.



Let's take a look at the files in our mounted directory.

# COMMAND ----------

# List Files in our Mounted Directory
dbutils.fs.ls("dbfs:/mnt/data/final/")

# COMMAND ----------

%md
### 2. Create a Dataframe

Let's start by creating a dataframe named tempDF with the bare minimum options, specifying the location of the file and that it's delimited.

The default delimiter for `spark.read.csv( )` is a comma but we could change it by specifying the option delimiter parameter.

Note: Today, we are working from CSVs that were mounted from blob storage, but there are many file types that can be accessed in Databricks. See the Databricks documentation for more information: https://docs.azuredatabricks.net/spark/latest/data-sources/index.html

# COMMAND ----------

csvFile = "/mnt/data/final/campaign_details_web.csv" # Create variable with link to our CSV file location
tempDF = (spark.read           # The DataFrameReader
   .option("header", "true")   # Use first line of all files as header
 # .option("delimiter", "\t")  # This is how we could pass in a Tab or other delimiter.
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

#View data in newly created DataFrame
display(tempDF) 

# COMMAND ----------

%md
### 3. View and Define Schemas
Let's view the **schema** for the DataFrame. 

The schema of a DataFrame is *a skeleton structure that represents the logical view of the data, including column names and their datatypes*. 

# COMMAND ----------

# View the schema of the tempDF DataFrame
tempDF.printSchema() 

# COMMAND ----------

%md
#### 3.1 Infer the Schema

All of the fields in our DataFrame are strings. In many cases, using the **inferSchema** option will help Spark assign the correct DataTypes for each column.

Let's create a new DataFrame and infer the schema.

# COMMAND ----------

campaignDetailsWeb = (spark.read            # The DataFrameReader
   .option("header", "true")       # Use first line of all files as header
   .option("inferSchema", "true")  # Automatically infer data types
   .csv(csvFile)                   # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

# View campaignDetailsWeb DataFrame 
display(campaignDetailsWeb)

# COMMAND ----------

%md
Let's review the inferred schema.

# COMMAND ----------

campaignDetailsWeb.printSchema()

# COMMAND ----------

%md
#### 3.2 Create a DataFrame with a User-Defined Schema

This time we are going to read the same file, but apply a schema that we define as the data is read in.

##### Benefits of User-Defined Schemas
* Inferring schemas of large files can be costly because Spark must read through all the data to determine the data type of each column.
* User-defined schemas ensure that data types are exactly what you want them to be, and you won't have to proof-check or convert data types after data is read.

# COMMAND ----------

%md
First, let's create a variable that contains the details for our declared schema.

# COMMAND ----------

from pyspark.sql.types import * # Required for StructField, StringType, IntegerType, etc.

csvSchema = StructType([ # Create a variable that contains our defined schema
  StructField("customer_id", IntegerType(), True),
  StructField("last_purchase", StringType(), True), 
  StructField("previous_spend", DecimalType(), True),
  StructField("mens", IntegerType(), True),
  StructField("womens", IntegerType(), True),
  StructField("customer_location", StringType(), True),
  StructField("newbie", IntegerType(), True),
  StructField("channel", StringType(), True),
  StructField("campaign", IntegerType(), True),
  StructField("dm_campaign", StringType(), True)
 ])

# COMMAND ----------

%md
Next, we will read in our data (and print the schema).

We can specify the schema, or rather the `StructType`, with the `schema(..)` command.

# COMMAND ----------

campaignDetailsWebFinal = (spark.read       # The DataFrameReader
  .option('header', 'true')   # Ignore line #1 - it's a header
  .schema(csvSchema)          # Use the specified schema
  .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

campaignDetailsWebFinal.printSchema()

# COMMAND ----------

display(campaignDetailsWebFinal)

# COMMAND ----------

%md
In this exercise, we learned how to read in data, create DataFrames, and define schemas. In our next lab, we will learn how to transform data and make it available for analysis.

# COMMAND ----------

%md
&copy; 2019 Insight Enterprises
