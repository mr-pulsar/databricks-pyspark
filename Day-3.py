# Databricks notebook source
df = spark.read.table('workspace.default.big_mart_sale')

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('Item_Type', regexp_replace('Item_Type','Dairy','Dai')).display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df1 = [(1, "Sanjay"), (2,"Dharun")]
sch1 = 'ID' 'INT', 'Name' 'STRING'
data=spark.createDataFrame(df1,sch1)
data.display()


# COMMAND ----------

df.select(sum('Item_MRP')).display()
df.select(min('Item_MRP')).display()
df.select(max('Item_MRP')).display()
df.select(avg('Item_MRP')).display()

# COMMAND ----------

spark.sql("select count(*) from workspace.default.big_mart_sale").display()

# COMMAND ----------

df11 = df.withColumn('Outlet_Type', split('Outlet_Type', ' '))
df11.display()

# COMMAND ----------

df11.withColumn('Outlet_Type', explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###**array_contains**

# COMMAND ----------

df11.withColumn('Outlet', array_contains('Outlet_Type', 'Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###**Group_By**

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()