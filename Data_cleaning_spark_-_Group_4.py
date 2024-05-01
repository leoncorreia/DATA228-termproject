#!/usr/bin/env python
# coding: utf-8

# # Cleaning the Prescriber Dataset

# In[ ]:


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RemoveColumnsExample") \
    .getOrCreate()

file_path = "C:/Users/Checkout/Desktop/Big Data/Project/Datasets/cms/Medicare Part D Prescribers - by Provider and Drug/MUP_DPR_RY23_P04_V10_DY21_NPIBN.csv"

df = spark.read.csv(file_path, header=True)

columns_to_remove = ['Prscrbr_State_FIPS', 'Prscrbr_Type_Src', 'Tot_30day_Fills', 'Tot_Benes', 'GE65_Sprsn_Flag', 'GE65_Tot_Clms', 'GE65_Tot_30day_Fills', 'GE65_Tot_Drug_Cst', 'GE65_Tot_Day_Suply', 'GE65_Bene_Sprsn_Flag', 'GE65_Tot_Benes']

filtered_df = df.drop(*columns_to_remove)

filtered_df.show(5)

output_file_path = "C:/Users/Checkout/Desktop/Big Data/Project/Datasets/cms/Medicare Part D Prescribers - by Provider and Drug/Medicare Part D Prescribers - Main.csv"
filtered_df.coalesce(1).write.csv(output_file_path, header=True, mode="overwrite")

# Stop SparkSession
spark.stop()


# # Joining All Payment Files to Make One Payment File

# In[ ]:


from pyspark.sql import SparkSession
from functools import reduce

spark = SparkSession.builder \
    .appName("CSV Join") \
    .getOrCreate()

directory_path = "C:/Users/Checkout/Desktop/Big Data/Project/Datasets/payments_main/"

files = [f"Payments_{year}.csv" for year in range(2017, 2023)]

dfs = []
for file in files:
    file_path = f"{directory_path}/{file}"
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    dfs.append(df)

appended_df = reduce(lambda df1, df2: df1.union(df2), dfs)

output_path = "C:/Users/Checkout/Desktop/Big Data/Project/Datasets/payments_main/Payments2017_2022.csv"
appended_df.coalesce(1).write.csv(output_path, mode="overwrite", header=True)

spark.stop()


# # Keeping only required columns in Payment File

# In[ ]:


spark = SparkSession.builder \
    .appName("Column Deletion") \
    .getOrCreate()

file_path = "C:/Users/Checkout/Desktop/Big Data/Project/Datasets/payments_main/Payments2017_2022.csv"

df = spark.read.csv(file_path, header=True, inferSchema=True)

columns_to_drop = [
    "Recipient_Country",
    "Physician_Primary_Type",
    "Physician_Specialty",
    "Record_ID",
    "Terms_of_Interest",
    "Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State",
    "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country",
    "Dispute_Status_for_Publication",
    "Interest_Held_by_Physician_or_an_Immediate_Family_Member"
]

df = df.drop(*columns_to_drop)

output_path = "C:/Users/Checkout/Desktop/Big Data/Project/Datasets/payments_main/Payments2017_2022_modified.csv"
df.coalesce(1).write.csv(output_path, mode="overwrite", header=True)

spark.stop()


# In[2]:


from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Column Rename and Save") \
    .getOrCreate()

# Define the file path
file_path = "C:/Users/Checkout/Desktop/Big Data/Project/Datasets/cms/Medicare Part D Prescribers - by Provider and Drug/Medicare Part D Prescribers - Main_modified.csv"

# Read the CSV file into a DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Define the new column names
new_column_names = {
    "Prscrbr_NPI": "Prescriber_NPI",
    "Prscrbr_Last_Org_Name": "Prescriber_Last_Organization_Name",
    "Prscrbr_First_Name": "Prescriber_First_Name",
    "Prscrbr_City": "City",
    "Prscrbr_State_Abrvtn": "State_Abbreviation",
    "Prscrbr_Type": "Prescriber_Type",
    "Brnd_Name": "Brand_Name",
    "Gnrc_Name": "Generic_Name",
    "Tot_Clms": "Total_Claims",
    "Tot_Day_Suply": "Total_Days_Supply",
    "Tot_Drug_Cst": "Total_Drug_Cost"
}

# Rename the columns
for old_col, new_col in new_column_names.items():
    df = df.withColumnRenamed(old_col, new_col)

# Define the output file path for the new CSV file
output_file_path = "C:/Users/Checkout/Desktop/Big Data/Project/Datasets/cms/Medicare Part D Prescribers - by Provider and Drug/Medicare Part D Prescribers - Main_renamed.csv"

# Write the DataFrame with renamed columns to a new CSV file
df.coalesce(1).write.csv(output_file_path, header=True, mode="overwrite")

# Stop the SparkSession
spark.stop()


# In[6]:


from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read Column Names") \
    .getOrCreate()

# File paths
file_path1 = "C:/Users/Checkout/Desktop/Big Data/Project/Datasets/cms/Medicare Part D Prescribers - by Provider and Drug/Medicare_provider_drug.csv"
file_path2 = "C:/Users/Checkout/Desktop/Big Data/Project/Datasets/cms/Medicare Part D Prescribers - by Provider and Drug/Medicare_main_renamed.csv"

# Read column names of the first CSV file (before cleaning)
columns_before_cleaning = spark.read.csv(file_path1, header=True).columns

# Read column names of the second CSV file (after cleaning)
columns_after_cleaning = spark.read.csv(file_path2, header=True).columns

# Print column names
print("Column names of Medicare file (before cleaning):", columns_before_cleaning)
print("\nColumn names of Medicare file (after cleaning):", columns_after_cleaning)

# Stop the SparkSession
spark.stop()


# In[ ]:




