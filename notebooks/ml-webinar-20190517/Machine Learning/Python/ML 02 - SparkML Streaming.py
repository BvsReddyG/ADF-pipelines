# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## SparkML on Streaming Data

# COMMAND ----------

# MAGIC %md
# MAGIC Let's take in the model we saved earlier, and apply it to some streaming data!

# COMMAND ----------

# MAGIC %run "./Includes/Classroom Setup"

# COMMAND ----------

# MAGIC %run "./Includes/Utility-Methods"

# COMMAND ----------

from pyspark.ml.pipeline import PipelineModel

fileName = userhome + "/tmp/DT_Pipeline"
pipelineModel = PipelineModel.load(fileName)

# COMMAND ----------

# MAGIC %md
# MAGIC We can simulate streaming data.
# MAGIC 
# MAGIC NOTE: You must specify a schema when creating a streaming source DataFrame.

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
  StructField("rating",DoubleType()), 
  StructField("review",StringType())])

streamingData = (spark
                 .readStream
                 .schema(schema)
                 .option("maxFilesPerTrigger", 1)
                 .parquet("/mnt/training/movie-reviews/imdb/imdb_ratings_50k.parquet"))

# COMMAND ----------

# MAGIC %md
# MAGIC Why is this stream taking so long? What configuration should we set?

# COMMAND ----------

stream = (pipelineModel
          .transform(streamingData)
          .groupBy("label", "prediction")
          .count()
          .sort("label", "prediction"))

display(stream, streamName="result_1p")

# COMMAND ----------

# MAGIC %md
# MAGIC Wait for the stream to start...

# COMMAND ----------

untilStreamIsReady("result_1p")

# COMMAND ----------

# MAGIC %md
# MAGIC Stop all the streams...

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's try this again

# COMMAND ----------

stream = (pipelineModel
          .transform(streamingData)
          .groupBy("label", "prediction")
          .count()
          .sort("label", "prediction"))

display(stream, streamName="result_2p")

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until the stream is read...

# COMMAND ----------

untilStreamIsReady("result_2p")

# COMMAND ----------

# MAGIC %md
# MAGIC Stop all the streams...

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC Let's save our results to a file.

# COMMAND ----------

import re

checkpointFile = userhome + "/tmp/checkPointPython"
dbutils.fs.rm(checkpointFile, True) # Clear out the checkpointing directory

(stream
 .writeStream
 .format("memory")
 .option("checkpointLocation", checkpointFile)
 .outputMode("complete")
 .queryName("result_3p")
 .start())

# COMMAND ----------

# MAGIC %md
# MAGIC Wait until the stream is ready...

# COMMAND ----------

untilStreamIsReady("result_3p")

# COMMAND ----------

# MAGIC %md
# MAGIC And now we can view the result

# COMMAND ----------

display(sql("select * from result_3p"))

# COMMAND ----------

# MAGIC %md
# MAGIC Stop all the streams...

# COMMAND ----------

for s in spark.streams.active:
  s.stop()


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>