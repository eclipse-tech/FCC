from pyspark.sql import SparkSession
from pyspark.sql.functions import col, months_between

# Create SparkSession
spark = SparkSession.builder.appName("WorkDurationExample").getOrCreate()

# Load the data from a source (e.g., CSV, Parquet, etc.)
sample_data = spark.read.csv("/Users/palakkothari/Desktop/FCC/sample-data.csv", header=True, inferSchema=True)


# Calculate the work duration in months for each site location
result = sample_data.withColumn("WorkDurationMonths",
                          months_between(col("end_timeline_work__c"), col("start_timeline_work__c")))

# Group by Site_Location__c and sum the work duration in months
result = result.groupBy("Site_Location__c").sum("WorkDurationMonths").alias("TotalWorkDurationMonths")

# Show the result
result.show(50)
