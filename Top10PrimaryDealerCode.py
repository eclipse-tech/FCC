from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc, col, collect_list

# Create a SparkSession
spark = SparkSession.builder.appName("read").getOrCreate()
# Read the data from the tables
dim_member = spark.read.csv("/Users/palakkothari/Desktop/FCC/member.csv", header=True, inferSchema=True)

# Find the primary dealer code with the maximum repetitions
max_repetitions = dim_member.groupBy("PrimaryDealerCode").count().orderBy(col("count").desc()).limit(10)

# Show the result
max_repetitions.show()
