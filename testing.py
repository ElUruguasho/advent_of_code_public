from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when, monotonically_increasing_id
from pyspark.sql.window import Window
import findspark
findspark.init()


spark = SparkSession.builder.getOrCreate()

data = [
    ("A",),
    ("B",),
    ("",),
    ("C",),
    ("D",),
    ("",),
    ("E",),
    ("F",),
]

schema = "value string"

df = spark.createDataFrame(data, schema=schema)

# Create a boolean column to mark empty strings
df = df.withColumn("is_empty", when(col("value") == "", 1).otherwise(0))
df = df.withColumn("idx", monotonically_increasing_id())

# Calculate the cumulative sum of the boolean column to create group IDs
window_spec = Window.orderBy("idx")
df = df.withColumn("group_id", _sum("is_empty").over(window_spec))

# Increment the group ID by 1 so that the first group has ID 1
df = df.withColumn("group_id", col("group_id") + 1)

df.show()

"""
Here ends the testing for challenge of Day1
"""
