# Databricks notebook source
# MAGIC %md 
# MAGIC # Machine Learning

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Lab Overview
# MAGIC 
# MAGIC In this lab, we will:
# MAGIC * Build a Pipeline for Feature Engineering
# MAGIC * Train a Spark ML Random Forest Model
# MAGIC * Evaluate the Model and Tune Parameters
# MAGIC * Log Experiments with MLflow

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Azure Databricks for Machine Learning and Data Scientists
# MAGIC ![arch](https://kpistoropen.blob.core.windows.net/collateral/roadshow/azure_roadshow_ml.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Steps
# MAGIC ### 1. Load Required Libraries
# MAGIC 
# MAGIC Let's load the required libraries for feature engineering and building the predictive model.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.ml.feature import QuantileDiscretizer, StringIndexer, OneHotEncoderEstimator, VectorAssembler, MinMaxScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.ml import Pipeline
from pyspark.ml.util import *
from pyspark.sql.functions import when, col, udf
from datetime import datetime

# For MLflow experiments
import os
import mlflow.spark
import mlflow.tracking

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read Data from Delta Tables
# MAGIC 
# MAGIC Use the Marketing Analysts Delta Table.

# COMMAND ----------

campaignsDF = spark.read.format("delta").load("/delta/analysts/")

display(campaignsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Feature Engineering
# MAGIC 
# MAGIC There are several steps we will need to perform to prepare the data for predictive modeling.
# MAGIC 
# MAGIC **NOTE:** One benefits of Spark ML is that some feature engineering can be initiated and then later chained together in a Pipeline, which is a workflow that can be used over and over again.
# MAGIC 
# MAGIC In the cells below, we will initiate fetaure engineering "transformers", but we will not execute the transformation yet. Instead, we will wrap them in a Pipeline (in the next step), and then use the Pipeline to execute them all.

# COMMAND ----------

# 3.1 Split into Train / Test
# Reserve 70% of the data for training and 30% for testing model performance.

df_train, df_val = (campaignsDF             # Define both train and test (validation) dataframes at the same time
                    .randomSplit([.7, .3],  # 70/30 split 
                                 seed=2019) # Same result every run
                   )


# 3.2 Balance Data
# Sample only 50% of the non-conversion records, and sample 100% of the conversion records. 

df_train = (df_train
            .sampleBy("conversion",              # Variable to sample by
                      fractions={0: 0.5, 1: 1},  # Proportion of non-conversions and conversions
                      seed = 2019)               # Same result every time
           )


# 3.3 Discretize
# Discretize the previous_spen feature into bins and create a new categorical feature with 5 categories of spend ranges.

discretizer = QuantileDiscretizer(
  numBuckets=5,
  inputCol="previous_spend",
  outputCol="previous_spend_binned"
)

# 3.4 String Index
# Spark ML models require that all categorical features are converted to integers. 

cat_features = ["previous_spend_binned",       # Define categorical features
                "mens",
                "womens",
                "customer_location",
                "newbie",
                "channel",
                "campaign"]

index_pipeline = Pipeline(stages = [           # Define the pipeline
  StringIndexer(
    inputCol= c,                               # Name of the columnn you will process
    outputCol= c+"_index",                     # Name of the columnn that will be output
    handleInvalid = "keep"                     # If the value is new, create a new index for it
  ) 
  for c in cat_features                        # Loop through creating the indexer for all categorical features
])

# 3.5 One-Hot Encoding
# Each category for each feature will be represented as a column vector of 1s and 0s that signify whether or not the record contains that category value.

encoder = OneHotEncoderEstimator(
  inputCols=[c + "_index" for c in cat_features], # Define categorical input columns with loop
  outputCols=[c + "_vec" for c in cat_features]   # Define categorical output columns with loop
)


# 3.6 Vector Assemble
# Spark ML models require that all features be vectorized. 
# All features will now be in a single vector, in a single column, for each record.

feature_columns = ["previous_spend",            # Define all feature columns, numeric and categorical
                   "previous_spend_binned_vec",
                   "mens_vec",
                   "womens_vec",
                   "customer_location_vec",
                   "newbie_vec",
                   "channel_vec",
                   "campaign_vec"]

vec_assembler = VectorAssembler(
  inputCols = feature_columns,
  outputCol='features'
)


# 3.7 Feature Scaling
# Improve model performance by scaling each feature to a number between 0 and 1.

scaler = MinMaxScaler(
  inputCol="features",
  outputCol="scaledFeatures"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Train the Predictive Model
# MAGIC 
# MAGIC #### 4.1 Set Parameters
# MAGIC We are going to set some parameters so we can tune them later to improve model performance.
# MAGIC 
# MAGIC There are several parameters you can define for a Random Forest model. 
# MAGIC 1. Number of Trees (numTrees) - How many decision trees does the forest create to make a prediction?
# MAGIC 2. Maximum Depth (maxDepth) - How many times can the decision trees split to get to the right answer?
# MAGIC 3. Maximum Bins (maxBins) - How many ways can each branch split (must be at least 2)?
# MAGIC 
# MAGIC For this exercise, let's start with some arbitrary good-start values for these parameters. 

# COMMAND ----------

numTrees = 25
maxDepth = 5
maxBins = 5

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2 Initialize the Model
# MAGIC 
# MAGIC In this step, we are only giving instructions to the algorithm and chaining it to the feature engineering steps. We will not train the model, yet.

# COMMAND ----------

rf = RandomForestClassifier(
  labelCol="conversion",             # Label we are trying to predict 
  featuresCol=scaler.getOutputCol(), # Feature names from last step of the pipeline
  numTrees=numTrees,                 # Parameters
  maxDepth=maxDepth,
  maxBins=maxBins
)

pipeline = Pipeline(stages=[
  discretizer,                       # Feature engineering steps
  index_pipeline,
  encoder,
  vec_assembler,
  scaler,
  rf                                 # Initialized model
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.3 Train the Model
# MAGIC 
# MAGIC In this step, all transformations are applied to our training data set, and the Random Forest algorithm learns patterns from the data.

# COMMAND ----------

model = pipeline.fit(df_train)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 5. Evaluate the Predictive Model
# MAGIC 
# MAGIC #### 5.1 Generate Predictions
# MAGIC In order to evaluate the model, we need to call it to make predictions on the test (validation) data.
# MAGIC 
# MAGIC Let's take a look at the validation dataset, including 
# MAGIC 1. the original features, 
# MAGIC 2. the predicted probability of purchase

# COMMAND ----------

predictionsDF = model.transform(df_val)

display(
  predictionsDF.select(
    "previous_spend",
    "previous_spend_binned",
    "mens",
    "womens",
    "customer_location",
    "newbie",
    "channel",
    "campaign",
    "conversion",
    "probability")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.2 Measure Performance
# MAGIC 
# MAGIC Let's use a performance metric called *Area Under the Receiver-Operator Characteristic Curve* or **AUC**.  This is a measure of how good the model is at distinguishing conversion vs non-conversion behavior.
# MAGIC 
# MAGIC We should strive to tune our model such that AUC is as close to 1 as possible. 

# COMMAND ----------

evaluator = BinaryClassificationEvaluator(
  rawPredictionCol="prediction",
  labelCol="conversion"
)

auc = round(evaluator.evaluate(predictionsDF), 2)
print("AUC: " + str(auc))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.3 Tune Model Parameters
# MAGIC 
# MAGIC Let's try another value for the parameters, and observe if if helps the performance on the test dataset.

# COMMAND ----------

numTrees = 30                                          # Redefine parameters
maxDepth = 10
maxBins = 10                                    

rf = RandomForestClassifier(                           # Re-initialize the model
  labelCol="conversion",
  featuresCol=scaler.getOutputCol(),
  numTrees=numTrees,
  maxDepth=maxDepth,
  maxBins=maxBins
)

pipeline = Pipeline(stages=[                           # Re-assemble the pipeline
  discretizer,
  index_pipeline,
  encoder,
  vec_assembler,
  scaler,
  rf
])

model = pipeline.fit(df_train)                         # Retrain the model

predictionsDF = model.transform(df_val)                # Make predictions

auc = round(evaluator.evaluate(predictionsDF),2)       # Calculate the performance metric

print("AUC: " + str(auc))                              # Print the performance metric

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Capture the Predictive Model in an Experiment
# MAGIC 
# MAGIC Up to this point, the models we have trained will be lost after our cluster is terminated. 
# MAGIC 
# MAGIC With MLflow, we can log, save, re-use, and compare models as "experiments". 
# MAGIC 
# MAGIC #### 6.1 Start an Experiment
# MAGIC 
# MAGIC Let's start an experiment to record a model training & evaluation. 
# MAGIC 
# MAGIC **NOTE:** Plug in *your* username for the path below.

# COMMAND ----------

mlflow.set_experiment("/Users/kristina.heinz@insight.com/final/marketing")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6.2 Record a Run
# MAGIC Now, we can take the entire process we used to train and evaluate our model and plug it inside a "run".
# MAGIC 
# MAGIC Along the way, we will log parameters, performance results, predictions, and then save the model file itself. 

# COMMAND ----------

# Redefine parameters
numTrees = 30
maxDepth = 10
maxBins = 10

# Start an MLflow run
with mlflow.start_run() as run:

  # Initialize the Random Forest Model
  rf = RandomForestClassifier(
    labelCol="conversion",
    featuresCol=scaler.getOutputCol(),
    numTrees=numTrees,
    maxDepth=maxDepth,
    maxBins=maxBins
  )
  # Chain together the feature engineering transformers and model
  pipeline = Pipeline(stages=[
    discretizer,
    index_pipeline,
    encoder,
    vec_assembler,
    scaler,
    rf
  ])
  
  # Train the model
  model = pipeline.fit(df_train) 
  
  # Make predictions
  predictionsDF = model.transform(df_val)
  
  # Evaluate
  auc = round(evaluator.evaluate(predictionsDF),2)
  
  # Log tags (telling us what problem we are solving for)
  mlflow.set_tag('domain', 'marketing')
  mlflow.set_tag('predict', 'probability of purchase')
  
  # Log a parameter (key-value pair)
  mlflow.log_param("numTrees", numTrees)
  mlflow.log_param("maxDepth", maxDepth)
  mlflow.log_param("maxBins", maxBins)
  
  # Log a metric - metrics can be updated throughout the run
  mlflow.log_metric("auc", auc)
  
  # Save the trained model and denote the parameters
  modelpath = (
    "/dbfs/marketing/model-%f-%f-%f" 
    % (round(numTrees),
       round(maxDepth),
       round(maxBins)
      )
  )
  
 # mlflow.spark.save_model(model, modelpath)
  
  mlflow.end_run()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6.3 Repeat  with Your Own Parameters
# MAGIC 
# MAGIC We've now compressed a lot of powerful machine learning techniques into just under 60 lines of code. 
# MAGIC 
# MAGIC Go back to set the parameters to anything you wish and rerun your experiment. Then, go check your experiment log in the UI.

# COMMAND ----------

mlflow.end_run()

# COMMAND ----------
