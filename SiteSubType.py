from pyspark.sql import SparkSession


# Create SparkSession
spark = SparkSession.builder.appName("CreatedMonthByAddress").getOrCreate()

# Load the data from a source (e.g., CSV, Parquet, etc.)
sample_data = spark.read.csv("/Users/palakkothari/Desktop/FCC/sample-data.csv", header=True, inferSchema=True)

# Select the Address__c and Created_Month__c columns
result = sample_data.select("Address__c", "site_sub_type__c")

result.show()

# Group the result by site_sub_type__c column
grouped_result = result.groupBy("site_sub_type__c").count()

# Show the grouped result
grouped_result.show()