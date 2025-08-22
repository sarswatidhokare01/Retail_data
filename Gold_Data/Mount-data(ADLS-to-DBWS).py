# Databricks notebook source
# MAGIC %md
# MAGIC ### Define Necessary Libraries 

# COMMAND ----------

storage_account_name = "retaildatasa"
container_name = "retail-data"
mount_point = "data"
client_id = "***********************"
tenant_id = "************************"
client_secret ="*****************************"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": f"{client_id}",
        "fs.azure.account.oauth2.client.secret": f"{client_secret}",
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount Storage Container

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{mount_point}",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### List Content of your mount point

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/data"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/data/silver-data"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check all the data present into the mounted folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Retail_data file
# MAGIC To check files we use pyspark 
# MAGIC
# MAGIC PySpark is the Python API for Apache Spark, a powerful open-source, distributed computing framework designed for big data processing and analytics.

# COMMAND ----------

retail_data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/data/silver-data/Retail_data.csv")

# COMMAND ----------

display(retail_data)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customer_data file

# COMMAND ----------

customer = spark.read.format("csv").option("header", "true").load("/mnt/data/silver-data/customer.csv")

# COMMAND ----------

display(customer)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orders_data file

# COMMAND ----------

orders = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/data/silver-data/order.csv")

# COMMAND ----------

display(orders)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product_data file

# COMMAND ----------

products = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/data/silver-data/product.csv")

# COMMAND ----------

products.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Primary Key 
# MAGIC If you are try to run join SQL query then one column should be your primary column and primary column does not have null values or duplicates so we have to remove duplicates from the primary column, primary column such as Transaction_ID (order_id)

# COMMAND ----------

Transaction_ID.dropDuplicates().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Remove Nulls and Duplicates from retail_data - 'order_id'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Retail_data Table**

# COMMAND ----------

retail_data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Remove NUll values and Duplicates from Transaction_ID column

# COMMAND ----------

from pyspark.sql.functions import col

df_retail_cleaned = retail_data.filter(col("Transaction_ID").isNotNull()) \
                             .dropDuplicates(["Transaction_ID"])

# COMMAND ----------

display(df_retail_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Customer Table**

# COMMAND ----------

customer.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### For Customer Table segarigate required fields from main table and make Customer_id as primary key also remove duplicates

# COMMAND ----------

df_customers = df_retail_cleaned.select("Customer_ID", "Name", "Email","Phone","Address","City","State","Zipcode","Country","Age","Gender","Income","Customer_Segment","Feedback","Ratings","Transaction_ID","product ID",).dropDuplicates(["customer_id"])

# COMMAND ----------

display(df_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Check any null values are present in customer table ?

# COMMAND ----------

from pyspark.sql.functions import col, sum

# Count of nulls in each column
df_customers.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_customers.columns]).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Renamed a column names in proper format

# COMMAND ----------

df_customers = df_customers.withColumnRenamed("Name", "customer_name")
df_customers = df_customers.withColumnRenamed("Email", "customer_email")
df_customers = df_customers.withColumnRenamed("Phone", "customer_phone")
df_customers = df_customers.withColumnRenamed("Transaction_ID", "order_id")
df_customers = df_customers.withColumnRenamed("product ID", "product_id")


# COMMAND ----------

df_customers.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Orders Table**

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### As Once we removed duplicateds and null from the Transaction_id column so here do not want to repeat the process again only select required fields from main table to Order table

# COMMAND ----------

df_orders = df_retail_cleaned.select("Transaction_ID","Date","Ship Date","Delivery Date","Year","Month","Time","Total_Purchases","Amount","Total_Amount","Shipping_Method","Payment_Method","Order_Status","Product ID","Customer_ID")

# COMMAND ----------

display(df_orders)

# COMMAND ----------

from pyspark.sql.functions import col, sum

# Count of nulls in each column
df_orders.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_orders.columns]).show()


# COMMAND ----------

df_orders = df_orders.withColumnRenamed("Transaction_ID", "order_id")
df_orders = df_orders.withColumnRenamed("Date", "order_date")
df_orders = df_orders.withColumnRenamed("Ship Date", "ship_date")
df_orders = df_orders.withColumnRenamed("Delivery Date", "delivery_date")
df_orders = df_orders.withColumnRenamed("Year", "order_year")
df_orders = df_orders.withColumnRenamed("Month", "order_month")
df_orders = df_orders.withColumnRenamed("Time", "order_time")
df_orders = df_orders.withColumnRenamed("Total_Purchases", "order_total_purchases")
df_orders = df_orders.withColumnRenamed("Amount", "price")
df_orders = df_orders.withColumnRenamed("Total_Amount", "order_total_amount")
df_orders = df_orders.withColumnRenamed("Shipping_Method", "shipping_method")
df_orders = df_orders.withColumnRenamed("Payment_Method", "payment_method")
df_orders = df_orders.withColumnRenamed("Order_Status", "order_status")

# COMMAND ----------

df_orders = df_orders.withColumnRenamed("Product ID", "product_id")
df_orders.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  **Products Table**

# COMMAND ----------

products.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### For Product Table segarigate required fields from main table and make Product_id as primary key also remove duplicates

# COMMAND ----------

df_products = df_retail_cleaned.select("Product ID","products","Product_Category","Product_Brand","Product_Type", "Customer_ID","Transaction_ID").dropDuplicates(["product ID"])

# COMMAND ----------

df_products.show(4)

# COMMAND ----------

df_products.count()

# COMMAND ----------

from pyspark.sql.functions import col, sum

# Count of nulls in each column
df_products.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_products.columns]).show()


# COMMAND ----------

df_products = df_products.withColumnRenamed("Product ID", "product_id")
df_products = df_products.withColumnRenamed("products", "product_name")


# COMMAND ----------

df_products = df_products.withColumnRenamed("Transaction_ID", "order_id")
df_products.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ####  registers the DataFrame df_customers, df_products, df_orders, df_retail_cleaned as a temporary SQL view 

# COMMAND ----------

df_customers.createOrReplaceTempView("df_customers")
df_products.createOrReplaceTempView("df_products")
df_orders.createOrReplaceTempView("df_orders")
df_retail_cleaned.createOrReplaceTempView("df_retail_cleaned")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Join Tables using PySpark SQL query
# MAGIC Join three tables such as Customer, Orders, Products and filters the results for customers in the 'Regular' segment using PySpark SQL query.

# COMMAND ----------

# Assuming df_customers is a DataFrame
df_customers.createOrReplaceTempView("df_customers")
df_orders.createOrReplaceTempView("df_orders")
df_products.createOrReplaceTempView("df_products")

# Now you can run the SQL query
query = """
select 
 df_customers.Customer_ID, 
 df_customers.City, 
 df_customers.Age, 
 df_customers.Gender, 
 df_products.product_id, 
 df_products.product_name,
 df_orders.order_date,
 df_orders.order_total_purchases 
from df_customers 
inner join df_products on df_customers.product_id = df_products.product_id
inner join df_orders on df_products.order_id = df_orders.order_id 
where df_customers.Customer_Segment = 'Regular'
"""
result = spark.sql(query)
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join products and orders Tables using PySpark's DataFrame API 

# COMMAND ----------

from pyspark.sql.functions import col

result_df = df_customers.join(df_products, df_customers.product_id == df_products.product_id, "inner") \
                        .join(df_orders, df_products.order_id == df_orders.order_id, "inner") \
                        .select(
                            df_customers.Customer_ID,
                            df_customers.City,
                            df_customers.Age,
                            df_customers.Gender,
                            df_products.product_id,
                            df_products.product_name,
                            df_orders.order_date,
                            df_orders.order_total_purchases
                        )

# To display the result
result_df.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Save transformed files into azure data lake storage
# MAGIC Convert that performed files into parquet format for less cost charges.
# MAGIC
# MAGIC Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval.

# COMMAND ----------

df_retail_cleaned.write.parquet("/mnt/data/gold-data/df_retail_cleaned.parquet")
df_orders.write.parquet("/mnt/data/gold-data/df_orders.parquet")
df_products.write.parquet("/mnt/data/gold-data/df_products.parquet")
df_customers.write.parquet("/mnt/data/gold-data/df_customer.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC Save files into csv to download into our local folder

# COMMAND ----------

df_orders.toPandas().to_csv("/dbfs/mnt/data/gold-data/csv/df_orders.csv", index=False)
df_products.toPandas().to_csv("/dbfs/mnt/data/gold-data/csv/df_products.csv", index=False)
df_retail_cleaned.toPandas().to_csv("/dbfs/mnt/data/gold-data/csv/df_retail_cleaned.csv", index=False)
df_customers.toPandas().to_csv("/dbfs/mnt/data/gold-data/csv/df_customers.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC - #### Display the data files mounted in the Gold layer.
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/data/gold-data"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display the data files mounted in the Silver layer.
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/data/silver-data"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Parquet files from the gold layer for further transformations

# COMMAND ----------

df_products = spark.read.parquet("/mnt/data/gold-data/df_products.parquet")
df_orders = spark.read.parquet("/mnt/data/gold-data/df_orders.parquet")
df_retail_cleaned = spark.read.parquet("/mnt/data/gold-data/df_retail_cleaned.parquet")
df_customers = spark.read.parquet("/mnt/data/gold-data/df_customer.parquet")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename the updated column name from Retail_data

# COMMAND ----------

df_retail_cleaned = df_retail_cleaned.withColumnRenamed("Transaction_ID", "order_id")
df_retail_cleaned = df_retail_cleaned.withColumnRenamed("Name", "customer_name")
df_retail_cleaned = df_retail_cleaned.withColumnRenamed("Date", "order_date")
df_retail_cleaned = df_retail_cleaned.withColumnRenamed("Total_Purchases", "order_total_purchases")
df_retail_cleaned = df_retail_cleaned.withColumnRenamed("Total_Amount", "order_total_amount")
df_retail_cleaned = df_retail_cleaned.withColumnRenamed("Year", "order_year")
df_retail_cleaned = df_retail_cleaned.withColumnRenamed("Month", "order_month")
df_retail_cleaned = df_retail_cleaned.withColumnRenamed("Time", "order_time")
df_retail_cleaned = df_retail_cleaned.withColumnRenamed("Amount", "order_amount")
df_retail_cleaned = df_retail_cleaned.withColumnRenamed("products", "product_name")
df_retail_cleaned = df_retail_cleaned.withColumnRenamed("Ship Date", "ship_date")
df_retail_cleaned = df_retail_cleaned.withColumnRenamed("Delivery Date", "delivery_date")
df_retail_cleaned = df_retail_cleaned.withColumnRenamed("Product ID", "product_id")

# COMMAND ----------

df_retail_cleaned = spark.read.parquet("/mnt/data/gold-data/df_retail_cleaned.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update Mounted file
# MAGIC If the file or table already exists, delete and replace it with the new one. use mode("overwrite) PySpark's DataFrameWriter API.

# COMMAND ----------

df_retail_cleaned = df_retail_cleaned.write.mode("overwrite").parquet("/mnt/data/gold-data/df_retail_cleaned.parquet")


# COMMAND ----------

df_retail_cleaned = spark.read.parquet("/mnt/data/gold-data/df_retail_cleaned.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Monthly Demand Sales Forecasting using linear regression

# COMMAND ----------

# MAGIC %md
# MAGIC ##### This shows:- Sales are dropping over time

# COMMAND ----------

from pyspark.sql.functions import col, date_format, to_date, sum as _sum
from pyspark.sql import Window
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

# Prepare Monthly Sales Data
df_retail_cleaned = df_retail_cleaned.withColumn('order_date', to_date(col('order_date')))
df_retail_cleaned = df_retail_cleaned.withColumn('order_month', date_format(col('order_date'), 'yyyy-MM'))
monthly_sales = df_retail_cleaned.groupBy('order_month').agg(_sum('order_total_amount').alias('order_total_amount'))

# Convert to Pandas DataFrame
monthly_sales_pd = monthly_sales.toPandas()

# Convert order_month to datetime and sort it
monthly_sales_pd['order_month'] = pd.to_datetime(monthly_sales_pd['order_month'], format='%Y-%m')
monthly_sales_pd = monthly_sales_pd.sort_values('order_month')

# Create numerical index
monthly_sales_pd['Month_Index'] = np.arange(len(monthly_sales_pd))
X = monthly_sales_pd[['Month_Index']]
y = monthly_sales_pd['order_total_amount']

# Train model
model = LinearRegression()
model.fit(X, y)

# Predict
monthly_sales_pd['Predicted_Sales'] = model.predict(X)

# Calculate difference
monthly_sales_pd['Difference'] = monthly_sales_pd['order_total_amount'] - monthly_sales_pd['Predicted_Sales']

# Evaluate model
rmse = np.sqrt(mean_squared_error(y, monthly_sales_pd['Predicted_Sales']))
print(f"📉 RMSE: {rmse:.2f}")

# Plot
plt.figure(figsize=(12, 6))
plt.plot(monthly_sales_pd['order_month'], y, label='Actual Sales', marker='o')
plt.plot(monthly_sales_pd['order_month'], monthly_sales_pd['Predicted_Sales'], label='Predicted Sales', linestyle='--')
plt.xlabel('Month')
plt.ylabel('Total Sales')
plt.title('Monthly Sales Forecasting')
plt.xticks(rotation=45)
plt.legend()
plt.grid()
plt.tight_layout()
plt.show()

# Show Difference Table
display(monthly_sales_pd[['order_month', 'order_total_amount', 'Predicted_Sales', 'Difference']])


# COMMAND ----------

# Plot Difference Bar Chart
plt.figure(figsize=(12, 6))
plt.bar(monthly_sales_pd['order_month'], monthly_sales_pd['Difference'], color=['green' if x >= 0 else 'red' for x in monthly_sales_pd['Difference']])
plt.axhline(0, color='black', linewidth=0.8)
plt.title('Difference Between Actual and Predicted Sales')
plt.xlabel('Month')
plt.ylabel('Difference (Actual - Predicted)')
plt.xticks(rotation=45)
plt.grid(axis='y')
plt.tight_layout()
plt.show()


# COMMAND ----------

df_retail_cleaned.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_retail_cleaned.columns]).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### **K-Means Clustering for Customer Segmentation**

# COMMAND ----------

df = spark.read.parquet("/mnt/data/gold-data/df_retail_cleaned.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Group customers into clusters based on their shopping behavior using K-Means
# MAGIC Customer segmentation means dividing customers into groups based on their behavior or profile (e.g., spend amount, age, income, purchase habits).
# MAGIC This helps businesses:
# MAGIC
# MAGIC * Understand different customer types
# MAGIC * Target the right people with marketing
# MAGIC * Personalize offers or pricing
# MAGIC * Improve customer service

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.clustering import KMeans

# Create Spark session
spark = SparkSession.builder.appName("CustomerSegmentation").getOrCreate()

# Load your data
df = spark.read.parquet("/mnt/data/gold-data/df_retail_cleaned.parquet", header=True, inferSchema=True)

# Group and aggregate to create customer features
agg_df = df.groupBy("Customer_ID").agg(
    {"order_total_amount": "sum",
     "order_id": "count",
     "order_amount": "avg",
     "order_total_purchases": "sum",
     "Ratings": "avg",
     "Age": "first",
     "Income": "first"}
).withColumnRenamed("sum(order_total_amount)", "Total_Amount_Spent") \
 .withColumnRenamed("count(order_id)", "Total_Transactions") \
 .withColumnRenamed("avg(order_amount)", "Average_Spend_Per_Transaction") \
 .withColumnRenamed("sum(order_total_purchases)", "Total_Quantity_Purchased") \
 .withColumnRenamed("avg(Ratings)", "Average_Rating") \
 .withColumnRenamed("first(Age)", "Customer_Age") \
 .withColumnRenamed("first(Income)", "Income")
 
# Encode Income (e.g., Low, Medium, High)
indexer = StringIndexer(inputCol="Income", outputCol="Income_Encoded")
agg_df = indexer.fit(agg_df).transform(agg_df)

# Feature assembly
feature_cols = [
    "Total_Amount_Spent", "Total_Transactions", "Average_Spend_Per_Transaction",
    "Total_Quantity_Purchased", "Average_Rating", "Customer_Age", "Income_Encoded"
]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_unscaled")
assembled = assembler.transform(agg_df)

# Scale features
scaler = StandardScaler(inputCol="features_unscaled", outputCol="features")
scaled_data = scaler.fit(assembled).transform(assembled)

# KMeans clustering
kmeans = KMeans(featuresCol='features', k=4, seed=1)
model = kmeans.fit(scaled_data)
clusters = model.transform(scaled_data)

# 8. Join clusters with original aggregated features
final_df = agg_df.join(clusters.select("Customer_ID", "prediction"), on="Customer_ID")

# 9. Show interpretation by averaging features in each cluster
final_df.groupBy("prediction").mean().show()


# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save Clustering Algorithm to gold layer 

# COMMAND ----------

customer_segmentation = final_df.write.mode("overwrite").parquet("/mnt/data/gold-data/customer-segmentation-KMeans.parquet")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Customer table

# COMMAND ----------

df_customers = spark.read.parquet("/mnt/data/gold-data/df_customer.parquet")
display(df_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Perform PySpark SQL query
# MAGIC The query fetches all records where the country is USA.
# MAGIC

# COMMAND ----------

# Assuming df_customers is a DataFrame
df_customers.createOrReplaceTempView("df_customers")

# Now you can run the SQL query
query = """
select Customer_ID, 
       customer_name, 
       State, Country, 
       Age, Gender, 
       Income, 
       Customer_Segment 
from df_customers 
where Country = 'USA'
"""
result = spark.sql(query)
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read all gold layer data to perform PySpark SQL Queries 

# COMMAND ----------

df_customers = spark.read.parquet("/mnt/data/gold-data/df_customer.parquet")
df_orders = spark.read.parquet("/mnt/data/gold-data/df_orders.parquet")
df_products = spark.read.parquet("/mnt/data/gold-data/df_products.parquet")


# Assuming df_customers is a DataFrame
df_customers.createOrReplaceTempView("df_customers")
df_orders.createOrReplaceTempView("df_orders")
df_products.createOrReplaceTempView("df_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Revenue by Product Category

# COMMAND ----------


# SQL to get revenue by product category
query = """
SELECT 
  p.Product_Category, 
  SUM(o.order_total_purchases) AS total_revenue
FROM df_orders o
JOIN df_products p ON o.product_id = p.product_id
GROUP BY p.Product_Category
ORDER BY total_revenue DESC
"""

result_df = spark.sql(query)
display(result_df)

# Convert to Pandas for plotting
pandas_df = result_df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualize Revenue by Product Category

# COMMAND ----------

import seaborn as sns

plt.figure(figsize=(8, 4))
sns.barplot(x='Product_Category', y='total_revenue', data=pandas_df, palette='viridis')
for index, value in enumerate(pandas_df['total_revenue']):
    plt.text(index, value, str(value), ha='center', va='bottom', fontsize=8)
plt.title('Total Revenue by Product Category')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Monthly Sales Trend (sales seasonality)

# COMMAND ----------

query_monthly_sales = """
SELECT 
  order_month, 
  SUM(order_total_purchases) AS total_sales
FROM df_orders
GROUP BY order_month
ORDER BY total_sales DESC
"""
monthly_df = spark.sql(query_monthly_sales).toPandas()
display(monthly_df)

# COMMAND ----------

plt.figure(figsize=(8, 4))
sns.barplot(x='order_month', y='total_sales', data=monthly_df, palette='crest')
for index, value in enumerate(monthly_df['total_sales']):
    plt.text(index, value, str(value), ha='center', va='bottom', fontsize=7)
plt.title('Monthly Sales Trend')
plt.xlabel('Month')
plt.ylabel('Total Sales')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Payment Method Usage

# COMMAND ----------

query_payment = """
SELECT 
  payment_method, 
  COUNT(*) AS payment_count
FROM df_orders
GROUP BY payment_method
ORDER BY payment_count DESC
"""
payment_df = spark.sql(query_payment).toPandas()
display(payment_df)

# COMMAND ----------

plt.figure(figsize=(8, 4))
sns.barplot(x='payment_method', y='payment_count', data=payment_df, palette='mako')
for index, value in enumerate(payment_df['payment_count']):
    plt.text(index, value, str(value), ha='center', va='bottom')
plt.title('Payment Method Distribution')
plt.ylabel('Number of Orders')
width = 0.5
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Revenue by City

# COMMAND ----------

query_city_revenue = """
SELECT 
  c.City, 
  SUM(o.order_total_purchases) AS total_revenue
FROM df_orders o
JOIN df_customers c ON o.Customer_ID = c.Customer_ID
GROUP BY c.City
ORDER BY total_revenue DESC
"""
city_df = spark.sql(query_city_revenue).toPandas()
display(city_df)


# COMMAND ----------

plt.figure(figsize=(8, 4))
sns.barplot(x='City', y='total_revenue', data=city_df.head(10), palette='coolwarm')
for index, value in enumerate(city_df['total_revenue'].head(10)):
    plt.text(index, value, str(value), ha='center', va='bottom', fontsize=7)
plt.title('Top 10 Cities by Revenue')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Top Product_Types Sold

# COMMAND ----------

query_product_type = """
SELECT 
  p.Product_Type, 
  COUNT(*) AS total_orders
FROM df_orders o
JOIN df_products p ON o.product_id = p.product_id
GROUP BY p.Product_Type
ORDER BY total_orders DESC
"""
product_type_df = spark.sql(query_product_type).toPandas()
display(product_type_df)


# COMMAND ----------

plt.figure(figsize=(8, 4.5))
sns.barplot(x='Product_Type', y='total_orders', data=product_type_df.head(10), palette='plasma')
for index, value in enumerate(product_type_df['total_orders'].head(10)):
    plt.text(index, value + 50, str(value), ha='center', va='bottom', fontsize=7)
plt.title('Top Product Types Ordered')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Customer Segment vs Total Revenue

# COMMAND ----------

query_segment_revenue = """
SELECT 
  c.Customer_Segment, 
  SUM(o.order_total_purchases) AS revenue
FROM df_orders o
JOIN df_customers c ON o.Customer_ID = c.Customer_ID
GROUP BY c.Customer_Segment
ORDER BY revenue DESC
"""
segment_df = spark.sql(query_segment_revenue).toPandas()
display(segment_df)

plt.figure(figsize=(7,4))
sns.barplot(x='Customer_Segment', y='revenue', data=segment_df, palette='flare')
for index, value in enumerate(segment_df['revenue']):
    plt.text(index, value, str(value), ha='center', va='bottom', fontsize=8)
plt.title('Revenue by Customer Segment')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Top Brands by Total Purchases

# COMMAND ----------

query_brand_sales = """
SELECT 
  p.Product_Brand, 
  SUM(o.order_total_purchases) AS total_sales
FROM df_orders o
JOIN df_products p ON o.product_id = p.product_id
GROUP BY p.Product_Brand
ORDER BY total_sales DESC
"""
brand_df = spark.sql(query_brand_sales).toPandas()
display(brand_df)

plt.figure(figsize=(7,4))
sns.barplot(x='Product_Brand', y='total_sales', data=brand_df.head(10), palette='Spectral')
for index, value in enumerate(brand_df['total_sales'].head(10)):
    plt.text(index, value + 50, str(value), ha='center', va='bottom', fontsize=7)
plt.title('Top 10 Brands by Sales')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Average Purchase per Customer

# COMMAND ----------

query_avg_purchase = """
SELECT 
  Customer_ID,
  AVG(order_total_purchases) AS avg_purchase
FROM df_orders
GROUP BY Customer_ID
"""
avg_df = spark.sql(query_avg_purchase).toPandas()
display(avg_df)

plt.figure(figsize=(7,4))
# Use only top 10 for both plotting and labels
top10_avg = avg_df.sort_values(by='avg_purchase', ascending=False).head(10)
sns.histplot(avg_df['avg_purchase'], bins=30, kde=True, color='skyblue')
for index, value in enumerate(top10_avg['avg_purchase']):
    plt.text(value, 0.2, f"{value:.2f}", ha='center', va='bottom', fontsize=7, rotation=90)
plt.title('Distribution of Average Purchase per Customer')
plt.xlabel('Average Purchase Amount')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Product Type vs Average Purchase Value

# COMMAND ----------

query_type_avg = """
SELECT 
  p.Product_Type,
  AVG(o.order_total_purchases) AS avg_purchase
FROM df_orders o
JOIN df_products p ON o.product_id = p.product_id
GROUP BY p.Product_Type
ORDER BY avg_purchase DESC
"""
type_avg_df = spark.sql(query_type_avg).toPandas()
display(type_avg_df)


# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/data/gold-data"
