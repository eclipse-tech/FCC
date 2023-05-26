from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, to_date
from pyspark.sql.functions import months_between
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DateType

# Create SparkSession
spark = SparkSession.builder.appName("WorkDurationExample").getOrCreate()

# Load the data from a source (e.g., CSV, Parquet, etc.)
sample_data = spark.read.csv("/Users/palakkothari/Desktop/FCC/sample-data.csv", header=True, inferSchema=True)

# Convert the timeline columns to DateType
sample_data = sample_data.withColumn("start_date", to_date(col("start_timeline_work__c"), "dd/MM/yy").cast(DateType()))
sample_data = sample_data.withColumn("end_date", to_date(col("end_timeline_work__c"), "dd/MM/yy").cast(DateType()))

# Calculate the ongoing and future projects based on the current date
current_date = current_date().cast(DateType())
ongoing_projects = sample_data.filter((col("start_date") <= current_date) & (col("end_date") >= current_date))


# Group the ongoing and future projects by city
ongoing_grouped = ongoing_projects.groupBy("Site_City__c").count().withColumnRenamed("count", "Ongoing_Projects")



# Show the result
ongoing_grouped.show()
