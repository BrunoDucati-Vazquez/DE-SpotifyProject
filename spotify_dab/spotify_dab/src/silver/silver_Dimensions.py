# Databricks notebook source
# MAGIC %md
# MAGIC # Read DimUser

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import os

project_path = os.path.join(os.getcwd(), "..", "..")
sys.path.append(project_path)
from utils.transformations import reusable


# COMMAND ----------

df_users = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", 
            "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/DimUser/checkpoint") \
    .load("abfss://bronze@saspotifyprojectbdv.dfs.core.windows.net/DimUser")

display(df_users)

# COMMAND ----------

df_users = df_users.withColumn("user_name", upper(col("user_name")))
display(df_users)

# COMMAND ----------

df_user_object = reusable()
df_users = df_user_object.dropColumns(df_users, ['_rescued_data'])
df_users = df_users.dropDuplicates(['user_id'])

# COMMAND ----------

display(df_users)

# COMMAND ----------

df_users.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/DimUser/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/DimUser/data")\
        .toTable("spotify.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimArtist**

# COMMAND ----------

df_artists = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", 
            "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/DimArtist/checkpoint") \
    .load("abfss://bronze@saspotifyprojectbdv.dfs.core.windows.net/DimArtist")

display(df_artists)

# COMMAND ----------

df_artist_object = reusable()
df_artists = df_artist_object.dropColumns(df_artists, ['_rescued_data'])
df_artists = df_artists.dropDuplicates(['artist_id'])

# COMMAND ----------

display(df_artists)

# COMMAND ----------

df_artists.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/DimArtist/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/DimArtist/data")\
        .toTable("spotify.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimTrack**

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", 
            "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/DimTrack/checkpoint") \
    .load("abfss://bronze@saspotifyprojectbdv.dfs.core.windows.net/DimTrack")

display(df_track)

# COMMAND ----------

df_track = df_track.withColumn("duration_flag", 
                               when(col('duration_sec') < 150, 'short')
                               .when((col('duration_sec') >= 150) & (col('duration_sec') < 300), 'medium')
                               .when(col('duration_sec') >= 300, 'long'))

df_track = df_track.withColumn("track_name", regexp_replace(col('track_name'), '-', ' '))

display(df_track)

# COMMAND ----------

df_track_object = reusable()
df_track = df_track_object.dropColumns(df_track, ['_rescued_data'])
df_track = df_track.dropDuplicates(['artist_id'])


# COMMAND ----------

display(df_track)

# COMMAND ----------

df_track.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/DimTrack/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/DimTrack/data")\
        .toTable("spotify.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimDate**

# COMMAND ----------

df_Date = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", 
            "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/DimDate/checkpoint") \
    .load("abfss://bronze@saspotifyprojectbdv.dfs.core.windows.net/DimDate")

display(df_Date)

# COMMAND ----------

df_date_object = reusable()
df_track = df_date_object.dropColumns(df_Date, ['_rescued_data'])
df_Date = df_Date.dropDuplicates(['date_key'])

display(df_Date)

# COMMAND ----------

df_Date.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/DimDate/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/DimDate/data")\
        .toTable("spotify.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **FactStream**

# COMMAND ----------

df_fact_stream = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", 
            "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/FactStream/checkpoint") \
    .load("abfss://bronze@saspotifyprojectbdv.dfs.core.windows.net/FactStream")

display(df_fact_stream)

# COMMAND ----------

df_fact_stream_object = reusable()
df_fact_stream = df_fact_stream_object.dropColumns(df_fact_stream, ['_rescued_data'])


# COMMAND ----------

df_fact_stream.writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/FactStream/checkpoint")\
        .trigger(once=True)\
        .option("path", "abfss://silver@saspotifyprojectbdv.dfs.core.windows.net/FactStream/data")\
        .toTable("spotify.silver.factStream")