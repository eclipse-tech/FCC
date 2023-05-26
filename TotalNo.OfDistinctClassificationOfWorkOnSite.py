from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc, col, count


# Create a SparkSession
spark = SparkSession.builder.appName("read").getOrCreate()

# Read the data from the tables
sample_data = spark.read.csv("/Users/palakkothari/Desktop/FCC/sample-data.csv", header=True, inferSchema=True)

# Group by distinct values and count occurrences
distinct_counts = sample_data.groupBy("Classification_of_work_on_site__c").count()

# Show the distinct values and their counts
distinct_counts.show()