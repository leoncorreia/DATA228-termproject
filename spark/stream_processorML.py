from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import col,broadcast
from pyspark import StorageLevel
import pandas as pd
from pyspark.ml.feature import Tokenizer, HashingTF, MinHashLSH
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
import os
import sys

spark = SparkSession.builder \
    .appName("PrescriptionDataProcessor") \
    .getOrCreate()

leie = spark.read.csv('leie.csv',header=True,escape="\"")
med_main = spark.read.csv('meds.csv',header=True,escape="\"")
payments = spark.read.csv('payment.csv',header=True,escape="\"")

med_main = med_main.withColumnRenamed("Prescriber_NPI", "NPI")
payments = payments.withColumnRenamed("Physician_NPI", "NPI")

leie_select = leie.select("NPI", "EXCLTYPE", "EXCLDATE")
med_main_select = med_main.select("NPI", "Total_Claims", "Total_Drug_Cost")
payments_select = payments.select("NPI", "Total_Amount_Invested_USDollars", "Value_of_Interest")


df_merged = leie_select.join(med_main_select, "NPI", "outer")\
                       .join(payments_select, "NPI", "outer")
df_merged = df_merged.na.fill({"EXCLTYPE": "No Exclusion"})


df_merged = df_merged.withColumn("Total_Claims", col("Total_Claims").cast("float")) \
                     .withColumn("Total_Drug_Cost", col("Total_Drug_Cost").cast("float")) \
                     .withColumn("Total_Amount_Invested_USDollars", col("Total_Amount_Invested_USDollars").cast("float")) \
                     .withColumn("Value_of_Interest", col("Value_of_Interest").cast("float"))

# Apply LSH on the 'EXCLTYPE' column
tokenizer = Tokenizer(inputCol="EXCLTYPE", outputCol="tokens")
hashingTF = HashingTF(inputCol="tokens", outputCol="rawFeatures", numFeatures=20)
mh = MinHashLSH(inputCol="rawFeatures", outputCol="hashes", numHashTables=5)

# Pipeline for hashing
pipeline = Pipeline(stages=[tokenizer, hashingTF, mh])
model = pipeline.fit(df_merged)
df_hashed = model.transform(df_merged)

# Prepare data for machine learning
df_ml = df_hashed.withColumn("label", when(col("EXCLTYPE") != "No Exclusion", 1).otherwise(0))

# Assemble features into a feature vector
feature_cols = ["Total_Claims", "Total_Drug_Cost", "Total_Amount_Invested_USDollars", "Value_of_Interest"]  # example feature columns
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_ml = assembler.transform(df_ml)

# Split the data into train and test sets
train_data, test_data = df_ml.randomSplit([0.7, 0.3], seed=42)

# Train a Logistic Regression model
lr = LogisticRegression(featuresCol="features", labelCol="label")
lrModel = lr.fit(train_data)

# Test the model
predictions = lrModel.transform(test_data)

# Evaluate the model
evaluator = BinaryClassificationEvaluator()
accuracy = evaluator.evaluate(predictions)

print("Accuracy of the model:",accuracy)