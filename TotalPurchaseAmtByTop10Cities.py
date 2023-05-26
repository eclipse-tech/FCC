from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, sum, col, when
import matplotlib.pyplot as plt

# Create a SparkSession
spark = SparkSession.builder.appName("JoinQuery").getOrCreate()

# Read the data from the tables
fact_scanned_data = spark.read.csv("/Users/palakkothari/Desktop/FCC/scanned.csv", header=True, inferSchema=True)
dim_member = spark.read.csv("/Users/palakkothari/Desktop/FCC/member.csv", header=True, inferSchema=True)
dim_material = spark.read.csv("/Users/palakkothari/Desktop/FCC/material.csv", header=True, inferSchema=True)

# Perform the join operations
#result = fact_scanned_data.join(dim_member, "MembershipNo") \
   # .join(dim_material, (fact_scanned_data["ItemCode"] == dim_material["MaterialCode"]) &
                       #(fact_scanned_data["ItemCode"] != "Unknown"))

result = dim_material.join(fact_scanned_data, (fact_scanned_data["ItemCode"] == dim_material["MaterialCode"]) &
                       (fact_scanned_data["ItemCode"] != "Unknown")) \
    .join(dim_member, "MembershipNo")


# Select the desired columns
result = result.select("SellingPrice", "MaterialCode", "PermanentCity", "ItemCode", "MembershipNo", "UpdatedDate")

# Replace specific words in PermanentCity column
result = result.withColumn(
    "PermanentCity",
    when(col("PermanentCity") == "DELHI", "NEW DELHI")
    .when(col("PermanentCity") == "Thane", "THANE")
    .when(col("PermanentCity") == "Mumbai", "MUMBAI")
    .when(col("PermanentCity") == "Jodhpur", "JODHPUR")
    .when(col("PermanentCity") == "Bangalore", "BANGALORE")
    .when(col("PermanentCity") == "Bengaluru", "BANGALORE")
    .when(col("PermanentCity") == "BENGALURU", "BANGALORE")
    .otherwise(col("PermanentCity"))
)

# Replace NULL values in SellingPrice column with zero
result = result.withColumn(
    "SellingPrice",
    when(col("SellingPrice").isNull(), 0.000)
    .otherwise(col("SellingPrice"))
)

# Aggregate the total selling price by PermanentCity
city_totals = result.groupby('PermanentCity').agg(sum("SellingPrice").alias("TotalSellingPrice"))
city_totals = city_totals.sort(desc("TotalSellingPrice"))


# Convert the selling prices to crore
city_totals = city_totals.withColumn("TotalSellingPrice", col("TotalSellingPrice"))

# Show the result
top_10_cities = city_totals.limit(10)
print("Total Purchase Amount by Top 10 Cities:")
top_10_cities.show()

city_names = [row["PermanentCity"] for row in top_10_cities.collect()]
prices = [row["TotalSellingPrice"] for row in top_10_cities.collect()]

plt.bar(city_names, prices)
plt.xlabel("PermanentCity")
plt.ylabel("Selling Price (in Crore)")
plt.title("Total Purchase Amount by Top 10 Cities:")
plt.xticks(rotation=90)
plt.show()




