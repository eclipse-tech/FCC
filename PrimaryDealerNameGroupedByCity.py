from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc, col, collect_list


# Create a SparkSession
spark = SparkSession.builder.appName("read").getOrCreate()

# Read the data from the tables
dim_member = spark.read.csv("/Users/palakkothari/Desktop/FCC/member.csv", header=True, inferSchema=True)

# Group by city and list primary dealer names
grouped_dealers = dim_member.groupBy("PermanentCity").agg(collect_list("PrimaryDealerName").alias("DealerNames"))

# Show the results
grouped_dealers.show()

