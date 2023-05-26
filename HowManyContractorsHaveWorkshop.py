from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, sum, col, when
import matplotlib.pyplot as plt

# Create a SparkSession
spark = SparkSession.builder.appName("READ").getOrCreate()

# Read the data from the tables
dim_member = spark.read.csv("/Users/palakkothari/Desktop/FCC/member.csv", header=True, inferSchema=True)

result = dim_member[dim_member["DoesHeHaveWorkshop"] == 1].select("DoesHeHaveWorkshop")
count = result.count()

print(count)