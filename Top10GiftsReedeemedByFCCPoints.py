from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
import matplotlib.pyplot as plt

spark = SparkSession.builder .master("local[3]") .appName("read csv") .getOrCreate()
#df = spark.read .format("csv").option("header", 'True').load("/Users/palakkothari/Desktop/FCC/fac1.csv")
df = spark.read.csv("/Users/palakkothari/Desktop/FCC/fac1.csv", header=True, inferSchema=True)

product_sales = df.groupBy("ProductName").sum("OrderQuantity")
sorted_products = product_sales.sort(desc("sum(OrderQuantity)"))
top_10_products = sorted_products.limit(10)
print("Top 10 gifts redeemed from FCC points:")
top_10_products.show()

product_names = [row["ProductName"] for row in top_10_products.collect()]
quantities_sold = [row["sum(OrderQuantity)"] for row in top_10_products.collect()]

plt.bar(product_names, quantities_sold)
plt.xlabel("Product")
plt.ylabel("Quantity Sold")
plt.title("Top 10 gifts redeemed from FCC points")
plt.xticks(rotation=90)
plt.show()



