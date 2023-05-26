from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, sum, col, when, count
import matplotlib.pyplot as plt

# Create a SparkSession
spark = SparkSession.builder.appName("read").getOrCreate()

# Read the data from the tables

dim_member = spark.read.csv("/Users/palakkothari/Desktop/FCC/member.csv", header=True, inferSchema=True)


# Filter the DataFrame where the number of carpenters is not NULL or zero
filtered_dim_member = dim_member.filter((dim_member["NoOfCarpenters"].isNotNull()) & (dim_member["NoOfCarpenters"] != 0))

# Count the distinct membership numbers
count_membership_numbers = filtered_dim_member.select("MembershipNo").distinct().count()

# Print the count of membership numbers
print("Toatl no. of contractors who have carpenters: ", count_membership_numbers)
