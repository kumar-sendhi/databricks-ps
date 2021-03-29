// Databricks notebook source
val configs = Map(
"dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
"dfs.adls.oauth2.client.id" -> "5ae5297f-d10a-419e-8eeb-3abba87611d3",
"dfs.adls.oauth2.credential" -> "7AV.C4Z7npQ-.TU2G0Pung-yr~q9lyBzV_",
"dfs.adls.oauth2.refresh.url" -> "https://login.microsoftonline.com/521ae675-49d9-4e53-9cfc-fd592a4d36c6/oauth2/token"
)

dbutils.fs.mount(
source = "adl://seyontaxisource.azuredatalakestore.net",
mountPoint = "/mnt/datalake",
extraConfigs = configs)

// COMMAND ----------

display(
dbutils.fs.ls("/mnt/datalake"))

// COMMAND ----------

dbutils.fs.head("/mnt/datalake/TaxiZones.csv")

// COMMAND ----------

val configs = Map(
"fs.azure.account.key.seyonsource.blob.core.windows.net" -> "1rxZ/vrHMXuHinIMoezEYX3/O59zhpIIUbA5ATGGxmaoh9+AYmCXXY0vBrvSjqkF+kSxdwH6w9yZhHpFKn0pZA=="
)

dbutils.fs.mount(
source = "wasbs://taxisource@seyonsource.blob.core.windows.net/",
mountPoint = "/mnt/storage",
extraConfigs = configs)