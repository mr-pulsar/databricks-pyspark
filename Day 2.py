# Databricks notebook source
df = spark.read.table('workspace.default.big_mart_sale')

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import  *

df.withColumn('Mul', col("Item_MRP")*2).display()

# COMMAND ----------

df.withColumn('Item_Weight',col("Item_Weight").cast('int')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###**regexp_replace**

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col("Item_Fat_Content"),'Regular','Reg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###**Aggregate(MIN, MAX, SUM, AVG, COUNT)**

# COMMAND ----------

df.select(sum(col("Item_MRP"))).display()

# COMMAND ----------

df.select(avg(col("Item_MRP"))).display()
df.select(max(col("Item_MRP"))).display()
df.select(min(col("Item_MRP"))).display()

# COMMAND ----------

df.filter(col("Item_Type") == 'Dairy').count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###**OrderBy/Sort**

# COMMAND ----------

df.sort(col("Item_Weight"),ascending=True).display()

# COMMAND ----------

df.sort(["Item_MRP", "Item_Visibility"], ascending= [0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###**Limit**

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

df.orderBy("Item_MRP", ascending=False).limit(5).display()

# COMMAND ----------

df.tail(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ###**Drop**

# COMMAND ----------

df.drop("Item_MRP", "Outlet_Identifier").display()

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.dropDuplicates(["Item_Type"]).display()

# COMMAND ----------

df.dropna().display()

# COMMAND ----------

df1=[(1,'Sanjay'),
    (2,'Kumar')]
schema1='ID INT, Name STRING'
ss1=spark.createDataFrame(df1,schema1)
ss1.display()


df2=[(3,"Dharun"),
    (2,"Kumar")]
schema2='ID INT, Name STRING'
ss2=spark.createDataFrame(df2,schema2)
ss2.display()

# COMMAND ----------

ds=ss1.union(ss2)
ds.display()

# COMMAND ----------

ds.dropDuplicates().display()

# COMMAND ----------

ss1.intersect(ss2).display()

# COMMAND ----------

data1 = [('1',"Nadu"),('2',"Mari")]
sch1 = "ID STRING, Name STRING"
df1=spark.createDataFrame(data1,sch1)
df1.display()
data2 = [("Nadu", '1'),("Mari", '3')]
sch2 = "Name STRING, ID STRING"
df2=spark.createDataFrame(data2,sch2)
df2.display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC `INITCAP(), UPPer(), LOWER()`

# COMMAND ----------

df.select(initcap('Item_Type')).display()

# COMMAND ----------

df = df.withColumn('Current_Date', current_date()).display()

# COMMAND ----------

df.withColumn('Current_Date',date_format('Current_Date','dd-MM-yyyy')).display()

# COMMAND ----------

df.withColumn('Item_Fat_Content', regexp_replace('Item_Fat_Content', 'Low Fat', 'L F')).display()

# COMMAND ----------

df.fillna('NA', subset=['Outlet_Size']).display()

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

df.withColumn('Current_Date',current_date()).display()