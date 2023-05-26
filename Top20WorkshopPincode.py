from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc, col, count


# Create a SparkSession
spark = SparkSession.builder.appName("read").getOrCreate()

# Read the data from the tables
dim_member = spark.read.csv("/Users/palakkothari/Desktop/FCC/member.csv", header=True, inferSchema=True)

# Find the top 10 pincodes with the highest number of workshops
top_10_pincode = dim_member.groupBy("WorkshopPincode").count().orderBy(desc("count")).limit(50)

# Show the results
top_10_pincode.show()