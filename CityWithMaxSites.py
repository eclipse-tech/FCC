from pyspark.sql import SparkSession
from pyspark.sql.functions import col, months_between, desc

# Create SparkSession
spark = SparkSession.builder.appName("WorkDurationExample").getOrCreate()

# Load the data from a source (e.g., CSV, Parquet, etc.)
sample_data = spark.read.csv("/Users/palakkothari/Desktop/FCC/sample-data.csv", header=True, inferSchema=True)
# Group by Site_City__c column and count occurrences
result = sample_data.groupBy("Site_City__c").count().orderBy(col("count").desc())

# Show the result
result.show()