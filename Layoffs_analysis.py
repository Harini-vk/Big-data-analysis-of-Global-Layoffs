# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("LayoffAnalysis").getOrCreate()

# df = spark.read.csv("layoffs.csv", header=True, inferSchema=True)
# df.show(5)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year
import pandas as pd
import matplotlib.pyplot as plt

# 1️⃣ Start Spark session
spark = SparkSession.builder.appName("LayoffAnalysis").getOrCreate()

# 2️⃣ Read the CSV file
# (make sure layoffs.csv is in the same folder as this Python file)
df = spark.read.csv("layoffs.csv", header=True, inferSchema=True)

print("=== Raw Data ===")
df.show(5)

# 3️⃣ Data Cleaning
# - Drop rows with null Company or Country
# - Convert Date column to DateType
df_clean = df.dropna(subset=["Company", "Country"])
df_clean = df_clean.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

# (HERE) Save cleaned data to HDFS
# df_clean.write.mode("overwrite").csv("hdfs://localhost:9000/layoffs_project/cleaned_data1") 


print("=== Cleaned Data ===")
df_clean.show(5)




import os

output_folder = "C:/Users/vhari/Desktop/big data/cleaned_data1"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

df_clean.write.mode("overwrite").csv(output_folder)
print(f"Cleaned data saved at: {output_folder}")





# 4️⃣ Add Year column for trend analysis
df_clean = df_clean.withColumn("Year", year(col("Date")))

# 5️⃣ Aggregations

# By Country
country_counts = (
    df_clean.groupBy("Country")
    .sum("Laid_Off")
    .withColumnRenamed("sum(Laid_Off)", "Total_Layoffs")
    .orderBy(col("Total_Layoffs").desc())
)

print("=== Total Layoffs by Country ===")
country_counts.show()

# By Industry
industry_counts = (
    df_clean.groupBy("Industry")
    .sum("Laid_Off")
    .withColumnRenamed("sum(Laid_Off)", "Total_Layoffs")
    .orderBy(col("Total_Layoffs").desc())
)

print("=== Total Layoffs by Industry ===")
industry_counts.show()

# By Year
yearly_counts = (
    df_clean.groupBy("Year")
    .sum("Laid_Off")
    .withColumnRenamed("sum(Laid_Off)", "Total_Layoffs")
    .orderBy("Year")
)

print("=== Total Layoffs by Year ===")
yearly_counts.show()

# 6️⃣ Convert to Pandas for plotting
pdf_country = country_counts.toPandas()
pdf_industry = industry_counts.toPandas()
pdf_yearly = yearly_counts.toPandas()

# 7️⃣ Visualization with Matplotlib

plt.figure(figsize=(10,6))
plt.bar(pdf_country["Country"], pdf_country["Total_Layoffs"])
plt.title("Layoffs by Country")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("layoffs_by_country.png")
plt.show()

plt.figure(figsize=(10,6))
plt.bar(pdf_industry["Industry"], pdf_industry["Total_Layoffs"])
plt.title("Layoffs by Industry")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("layoffs_by_industry.png")
plt.show()

plt.figure(figsize=(8,5))
plt.plot(pdf_yearly["Year"], pdf_yearly["Total_Layoffs"], marker="o")
plt.title("Layoffs by Year")
plt.xlabel("Year")
plt.ylabel("Total Layoffs")
plt.grid(True)
plt.tight_layout()
plt.savefig("layoffs_by_year.png")
plt.show()

# 8️⃣ Stop Spark
spark.stop()
