from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

# -----------------------------------------
# 1️⃣ Create Spark Session
# -----------------------------------------
spark = SparkSession.builder \
    .appName("Layoffs Data Cleaning") \
    .getOrCreate()

# -----------------------------------------
# 2️⃣ Load raw CSV data from local file
# -----------------------------------------
# Reading from local file instead of HDFS (no upload needed)
df = spark.read.option("header", "true").csv("layoffdataset.csv")  # Assumes file is in the current directory

print("Raw data loaded successfully!")
print("Total Rows:", df.count())
print("Columns:", df.columns)

# -----------------------------------------
# 3️⃣ Data Cleaning
# -----------------------------------------

# Step 1: Remove duplicate rows
df = df.dropDuplicates()

# Step 2: Drop rows with all NULL values
df = df.na.drop(how="all")

# Step 3: Trim extra spaces in string columns
string_cols = [field.name for field in df.schema.fields if field.dataType.simpleString() == 'string']
for c in string_cols:
    df = df.withColumn(c, trim(col(c)))

# Step 4: Drop irrelevant columns
columns_to_drop = ["List_of_Employees_Laid_Off", "Source", "Date_Added"]
for col_name in columns_to_drop:
    if col_name in df.columns:
        df = df.drop(col_name)

# Step 5: Handle missing values in numeric columns
if "Percentage" in df.columns:
    df = df.na.fill({"Percentage": "0"})

# Step 6: Convert numeric columns to proper types
if "Percentage" in df.columns:
    df = df.withColumn("Percentage", col("Percentage").cast("double"))
if "Laid_Off_Count" in df.columns:
    df = df.withColumn("Laid_Off_Count", col("Laid_Off_Count").cast("int"))
if "Funds_Raised" in df.columns:
    df = df.withColumn("Funds_Raised", col("Funds_Raised").cast("double"))

# -----------------------------------------
# 4️⃣ Show sample cleaned data
# -----------------------------------------
print("\nSample Cleaned Data:")
df.show(10)

# -----------------------------------------
# 5️⃣ Save cleaned data to local directory
# -----------------------------------------
output_path = "layoffs_cleaned"  # Saves to a local subdirectory (e.g., C:\Users\vhari\Desktop\big data\layoffs_cleaned)
df.write.mode("overwrite").option("header", "true").csv(output_path)

print(f"\nCleaned dataset saved locally at: {output_path}")

# -----------------------------------------
# 6️⃣ Stop Spark session
# -----------------------------------------
spark.stop()