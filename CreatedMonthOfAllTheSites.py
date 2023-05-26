from pyspark.sql import SparkSession


# Create SparkSession
spark = SparkSession.builder.appName("CreatedMonthByAddress").getOrCreate()

# Load the data from a source (e.g., CSV, Parquet, etc.)
sample_data = spark.read.csv("/Users/palakkothari/Desktop/FCC/sample-data.csv", header=True, inferSchema=True)

# Select the Address__c and Created_Month__c columns
result = sample_data.select("Address__c", "Created_Month__c")

# Show the result
result.show()