# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE IF EXISTS database_mattia_demos CASCADE;
# MAGIC CREATE DATABASE database_mattia_demos;
# MAGIC USE database_mattia_demos

# COMMAND ----------

def generate_user_names(faker):
  return faker.first_name(), faker.last_name()

# COMMAND ----------

import random
import string
from faker import Faker

def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))
  
def create_data():
  data = []
  faker = Faker()
  for index in range(0, 1000):
    first_name, last_name = generate_user_names(faker)
    data.append([index, "{0} {1}".format(first_name, last_name), random.randint(1,100), "{0}.{1}@gmail.com".format(first_name, last_name)])

  return spark.createDataFrame(data, ["ID", "NAME", "AGE", "EMAIL-PII"]).write.format("delta").saveAsTable("test_restore")

# COMMAND ----------

create_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE test_restore

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

dataframe = spark.createDataFrame([[1000000, "Mattia Zeni", 33, "mattia.zeni@databricks.com", "RANDOM"]], ["ID", "NAME", "AGE", "EMAIL-PII", "NEW_COLUMN"])

# COMMAND ----------

dataframe.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("test_restore")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY test_restore

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM test_restore VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM test_restore VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM test_restore

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC RESTORE TABLE test_restore TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY test_restore

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM test_restore
