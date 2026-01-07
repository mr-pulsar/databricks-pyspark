# Databricks notebook source
df = spark.read.table('workspace.default.big_mart_sale')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

df.select('Item_Fat_Content').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###**Alias**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df.select(col("Item_Identifier")).display

df.select(col("Item_Identifier").alias("Item_ID")).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###**Filter**

# COMMAND ----------

# Scenario 1
df.display()


# COMMAND ----------



# COMMAND ----------

df.filter(col("Item_MRP")<=141.618).display()

# COMMAND ----------

# Scenario 2

df.filter((col("Item_Type")=='Soft Drinks') & (col("Item_Weight")<10)).display()

# COMMAND ----------

ss = df.withColumn("Item_Weight", col("Item_Weight").cast("int"))


# COMMAND ----------

ss.display()

# COMMAND ----------

df.withColumn("Item_MRP",col("Item_MRP")*23).display()

# COMMAND ----------

# Scenario 3

# df.filter((col("Outlet_Size").isNull())&(col('Outlet_Location_Type').isin(['Tier 1','Tier 2']))).display()


df.filter(col("Outlet_Size").isNull()).count()


# COMMAND ----------

# MAGIC %md
# MAGIC ###**WithColumnRenamed**

# COMMAND ----------

df.withColumnRenamed('Item_Fat_Content','Item_Fat').display()

# COMMAND ----------

df.filter(col('Item_Weight')>=12).display()

# COMMAND ----------

