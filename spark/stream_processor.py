from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Create SparkSession
spark = SparkSession.builder \
    .appName("PrescriptionDataProcessor") \
    .getOrCreate()


meds = spark.read.csv("./meds.csv", header=True, inferSchema=True)
# Kafka configurations
kafka_bootstrap_servers = "kafka:9092"

# Define the Kafka topics for each CSV file
kafka_topics = {
    # "meds": "meds",
    "leie": "leie",
    "payment": "payment"
}

# Define schemas for each CSV data
schemas = {
    # "meds": StructType([
    #     StructField("Prescriber_NPI", StringType(), True),
    #     StructField("Prescriber_Last_Organization_Name", StringType(), True),
    #     StructField("Prescriber_First_Name", StringType(), True),
    #     StructField("City", StringType(), True),
    #     StructField("State_Abbreviation", StringType(), True),
    #     StructField("Prescriber_Type", StringType(), True),
    #     StructField("Brand_Name", StringType(), True),
    #     StructField("Generic_Name", StringType(), True),
    #     StructField("Total_Claims", IntegerType(), True),
    #     StructField("Total_Days_Supply", IntegerType(), True),
    #     StructField("Total_Drug_Cost", FloatType(), True)
    # ]),
    "leie": StructType([
        StructField("LASTNAME", StringType(), True),
        StructField("FIRSTNAME", StringType(), True),
        StructField("MIDNAME", StringType(), True),
        StructField("BUSNAME", StringType(), True),
        StructField("GENERAL", StringType(), True),
        StructField("SPECIALTY", StringType(), True),
        StructField("UPIN", StringType(), True),
        StructField("NPI", StringType(), True),
        StructField("DOB", StringType(), True),
        StructField("ADDRESS", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("ZIP", StringType(), True),
        StructField("EXCLTYPE", StringType(), True),
        StructField("EXCLDATE", StringType(), True),
        StructField("REINDATE", StringType(), True),
        StructField("WAIVERDATE", StringType(), True),
        StructField("WVRSTATE", StringType(), True)
    ]),
    "payment": StructType([
        StructField("Physician_Profile_ID", StringType(), True),
        StructField("Physician_NPI", StringType(), True),
        StructField("Physician_First_Name", StringType(), True),
        StructField("Physician_Last_Name", StringType(), True),
        StructField("Recipient_City", StringType(), True),
        StructField("Recipient_State", StringType(), True),
        StructField("Program_Year", StringType(), True),
        StructField("Total_Amount_Invested_USDollars", FloatType(), True),
        StructField("Value_of_Interest", FloatType(), True)
    ])
}

# Read from Kafka for each topic and create final DataFrames
final_dfs = {}
for topic_name, kafka_topic in kafka_topics.items():
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .load()

    # Parse the value column as CSV
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .withColumn("csv", from_json(col("value"), schemas[topic_name])) \
        .select("csv.*")

    final_dfs[topic_name] = parsed_df

# Write the data to in-memory DataFrames
for topic_name, final_df in final_dfs.items():
    query = final_df.writeStream \
        .format("memory") \
        .queryName(f"{topic_name}_data") \
        .outputMode("append") \
        .option("checkpointLocation", f"/tmp/{topic_name}_checkpoint").start()
print(final_dfs['payment']['Value_of_Interest'][0])
print(meds.printSchema())
# Wait for the termination of all streaming queries
spark.streams.awaitAnyTermination()
