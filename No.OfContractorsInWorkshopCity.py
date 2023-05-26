from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, desc

# Create a SparkSession
spark = SparkSession.builder.appName("READ").getOrCreate()

# Read the data from the tables
dim_member = spark.read.csv("/Users/palakkothari/Desktop/FCC/member.csv", header=True, inferSchema=True)

# Group the data by WorkshopCity and count the distinct MembershipNo
membership_count = dim_member.groupBy("WorkshopCity").agg(countDistinct("MembershipNo").alias("Contractors")).orderBy(col("Contractors").desc())

# Show the results
membership_count.show(30)
