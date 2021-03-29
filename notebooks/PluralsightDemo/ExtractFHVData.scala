// Databricks notebook source
var fhvTaxiTripDataDF_InferSchema = spark.
read.
option("header","true").
option("inferSchema","true").
csv("/mnt/storage/fhvhv_tripdata_2020-01.csv")

// COMMAND ----------

// MAGIC %fs ls /mnt/storage

// COMMAND ----------

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

val fhvTaxiTripSchema = StructType(
List(
StructField("pickup_datetime", TimestampType, true),
StructField("dropoff_datetime", TimestampType, true),
StructField("PULocationID", IntegerType, true),
  StructField("DOLocationID", IntegerType, true),
  StructField("SR_Flag", IntegerType, true),
  StructField("dispatching_base_num", StringType, true),
  StructField("dispatching_base_number", StringType, true),
))

// COMMAND ----------

var fhvTaxiTripDataDF = spark.
read.
option("header","true").
schema(fhvTaxiTripSchema).
csv("/mnt/storage/fhvhv_tripdata_2020-01.csv")

// COMMAND ----------

var fhvTaxiTripDataDF = spark.
read.
option("header","true").
schema(fhvTaxiTripSchema).
csv("/mnt/storage/fhvhv_tripdata_*.csv")