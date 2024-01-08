from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType
import pymysql
from analytic import aggregate_csv_data
#conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
#cursor = conn.cursor()

def insert_into_phpmyadmin(row):
    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    database = "crimes_analysis"
    username = "root"
    password = ""
    
    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    # Extract the required columns from the row
    column1_value = row['Crime']
    column2_value = row['Location']
    column3_value = row['Count']
    #column4_value = row['Crime']
    #column5_value = row['Reporting Area']
    #column6_value = row['Neighborhood']
    #column7_value = row['Location']
    # Prepare the SQL query to insert data into the table
    #sql_query = fr"""INSERT INTO output crimes (File Number , Date of Report , Crime Date Time, Crime , Reporting Area, Neighborhood, Location ) VALUES ('{column1_value}', '{column2_value}','{column3_value}','{column4_value}','{column5_value}','{column6_value}','{column7_value}')"""
    sql_query = fr"""INSERT INTO crimes (Crime, Location, Count) VALUES ("{column1_value}", "{column2_value}", "{column3_value}")"""
    # Execute the SQL query
    cursor.execute(sql_query)

    # Commit the changes
    conn.commit()
    conn.close()

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
#schema = StructType().add("File Number", StringType()).add("Date of Report", StringType()).add("Crime Date Time", StringType()).add("Crime",StringType()).add("Reporting Area",IntegerType()).add("Neighborhood",StringType()).add("Location",StringType())
schema = StructType().add("Crime" ,StringType()).add("Location" , StringType()).add("Count" , IntegerType())

# Read data from Kafka topic as a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crimes") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \

# Select specific columns from "data"
#df = df.select("data.File Number" , "data.Date of Report" , "data.Crime Date Time", "data.Crime" , "data.Reporting Area", "data.Neighborhood", "data.Location")
df = df.select("data.Crime" , "data.Location" , "data.Count")
# Convert the value column to string and display the result
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreach(insert_into_phpmyadmin) \
    .start()

# Wait for the query to finish
query.awaitTermination()
