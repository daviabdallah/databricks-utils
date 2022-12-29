# Databricks notebook source
# MAGIC %md
# MAGIC # Manipulate CSV using DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate DELTA from CSV

# COMMAND ----------

df = spark.read.option("header", "true").option("delimiter", "|").option("inferSchema", "false").csv('wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/flights/departuredelays.csv')

df.write.mode("overwrite").format("delta").save('/mnt/<REPLACE TO YOUR DELTA PATH>')

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELECT FROM DELTA to inspect data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT recdate, tbl.PharmNPItype
# MAGIC FROM
# MAGIC DELTA.`/mnt/<REPLACE TO YOUR DELTA PATH>` AS tbl

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPDATE DELTA using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE
# MAGIC DELTA.`/mnt/<REPLACE TO YOUR DELTA PATH>` AS tbl
# MAGIC SET delay = -1
# MAGIC WHERE date < '2022-21-31';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate CSV from DELTA

# COMMAND ----------

df2 = spark.read.format("delta").load('.`/mnt/<REPLACE TO YOUR DELTA PATH>')

# update CSV data location for new file
dst_data_location = '/mnt/<REPLACE TO YOUR CSV PATH>'

df2.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").save(dst_data_location)


files = dbutils.fs.ls(dst_data_location)
csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
print(csv_file)
dbutils.fs.mv(csv_file, dst_data_location.rstrip('/') + ".csv")
dbutils.fs.rm(dst_data_location, recurse = True)
