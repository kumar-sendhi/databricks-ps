// Databricks notebook source
display(
dbutils.fs.ls("/mnt/datalake"))

// COMMAND ----------

// MAGIC %fs ls /mnt/datalake

// COMMAND ----------

// MAGIC %fs head /mnt/datalake/green_tripdata_2020-01.csv

// COMMAND ----------

var yellowTaxiTripDataDF = spark.read.option("header","true").csv("/mnt/datalake/green_tripdata_2020-01.csv")

// COMMAND ----------

display(
yellowTaxiTripDataDF)

// COMMAND ----------

// MAGIC %fs head /mnt/datalake/GreenTaxiTripData_201812.csv

// COMMAND ----------

var greenTaxiTripDataDF = spark.read.option("delimiter","\t").csv("/mnt/datalake/GreenTaxiTripData_201812.csv")

// COMMAND ----------

display(
greenTaxiTripDataDF)

// COMMAND ----------

// MAGIC %fs head /mnt/datalake/PaymentTypes.json

// COMMAND ----------

// MAGIC %fs ls /mnt/datalake

// COMMAND ----------

var paymentTypesDF = spark.read.json("/mnt/datalake/PaymentTypes.json")

// COMMAND ----------

display(paymentTypesDF)