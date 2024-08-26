# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "3b42de2e-d0c6-49e3-b7a7-838cb6d2abcb",
"fs.azure.account.oauth2.client.secret": '42X8Q~rek8eE5QLqEzmnRAlcQopN~nqYuiiEfcMb',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d0a26d55-d508-4924-ba9d-8f93f8c166f1/oauth2/token"}


dbutils.fs.mount(
source = "abfss://mfdatasetrawsource@mfdatastorageaccount.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/MutualFunds",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/MutualFunds"

# COMMAND ----------

mfdetails = spark.read.format("csv").option("header","true").load("/mnt/MutualFunds/raw_data/Expanded_FundDetails.csv")
mfnav = spark.read.format("csv").option("header","true").load("/mnt/MutualFunds/raw_data/Expanded_FundNAV.csv")
mfmarketindex = spark.read.format("csv").option("header","true").load("/mnt/MutualFunds/raw_data/Expanded_MarketIndex.csv")


# COMMAND ----------

mfdetails.show()
mfnav.show()
mfmarketindex.show()

# COMMAND ----------

mfdetails.printSchema()
mfnav.printSchema()
mfmarketindex.printSchema()

# COMMAND ----------

mfnav.write.option("header","true").csv("/mnt/MutualFunds/transformed_data/Expanded_FundNAV")
mfmarketindex.write.option("header","true").csv("/mnt/MutualFunds/transformed_data/Expanded_MarketIndex")
