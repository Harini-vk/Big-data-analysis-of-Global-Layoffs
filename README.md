# Hiring to Firing: Big Data Analysis of Global Job Layoffs

This project performs a **big data analysis of global job layoffs** to identify trends, patterns, and impacts across companies, industries, countries, and time periods.

The dataset is processed using **Apache Hadoop and Apache Spark**, and the final insights are visualized using **Power BI**.

---

## 🎯 Project Objective

- Analyze large-scale global layoffs data using big data technologies
- Perform distributed data cleaning and transformation
- Identify layoff trends across years, industries, companies, and countries
- Visualize insights using interactive dashboards

---

## 🧰 Tech Stack

### Big Data & Processing
- **Apache Hadoop (HDFS)**
- **Apache Spark (PySpark)**

### Programming Language
- **Python**

### Visualization
- **Power BI**

---

## 📁 Dataset Description

The dataset contains global job layoff information, including:
- Company name
- Industry
- Country
- Number of employees laid off
- Percentage of workforce laid off
- Funding details
- Year of layoff

The raw dataset is large and requires preprocessing before analysis.

---

## 🔄 Project Workflow (ETL Pipeline)

### 1️⃣ Extract
- Raw layoffs dataset stored in Hadoop Distributed File System (HDFS)
- Hadoop services (NameNode and DataNode) were configured and running

### 2️⃣ Transform
Data cleaning and preprocessing were performed using **Apache Spark**, including:
- Removal of duplicate records
- Dropping rows with all NULL values
- Trimming extra spaces from string columns
- Handling missing values
- Data type conversions for numeric columns
- Removal of irrelevant columns

### 3️⃣ Load
- The processed dataset was saved in HDFS
- The cleaned output was downloaded from HDFS to the local system
- The final dataset was imported into Power BI for visualization

---

## ⚙️ Data Processing Using Spark

Apache Spark was used to handle data cleaning efficiently in a distributed environment.  
Spark was chosen over traditional tools to ensure scalability when working with large datasets.

Key operations include:
- `dropDuplicates()`
- `na.drop()`
- `withColumn()`
- Type casting and transformations
- Writing cleaned data for analysis

---

## 📊 Power BI Dashboard

The cleaned dataset was visualized in Power BI to generate insights such as:
- Layoffs by company (percentage)
- Layoffs by country (percentage and count)
- Industry-wise analysis using slicers
- Year-wise layoff trends
- Total number of employees laid off globally

### Key KPI:
- **Total Employees Laid Off Globally: 413,000+**

---

## 🧠 Key Insights

- Layoffs peaked significantly around **2022**
- The **United States** accounts for the highest number of layoffs
- Certain companies experienced very high layoff percentages
- Layoff impact varies significantly across industries
- Over **413K employees** were affected globally

---

## 📌 Why Hadoop & Spark?

- Efficient handling of large datasets
- Distributed and parallel data processing
- Faster transformations compared to traditional tools
- Industry-standard big data ecosystem

---

## ⚠️ Disclaimer

This project is developed for academic and analytical purposes.  
Insights are based on the available dataset and may not fully represent real-world conditions.

---
![WhatsApp Image 2026-01-01 at 10 40 12](https://github.com/user-attachments/assets/77d11d0b-3805-4216-8678-b65704835c8d)


