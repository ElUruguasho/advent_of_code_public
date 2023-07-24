from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
import findspark
findspark.init()
  
spark = SparkSession.builder.getOrCreate()
  
input = spark.read.format("text").load("C:/Python_projects/advent_of_code/Day1/Day1_input.txt")

# Each blank space is separating one elf from the other  
# input.show()
#input.printSchema()

# Checks if the nulls are considered string or not
# input.filter("value is NULL").show()
# input.printSchema()

# conditional field to identify which are the int values and which are the empty strings
condition_is_int = (when(col('value') == '', "empty_value")
                  .otherwise("Calories"))

is_integer = input.withColumn("is_integer", condition_is_int)

#condition to cast into int afterwards
convert_to_int = (when(col('is_integer') == "Calories", col('value').cast(IntegerType()))
                .otherwise(0))

corrected_values = is_integer.withColumn("correct_value", convert_to_int)

# provides an id for each row which is needed so the window partition ordering does not impact the structure of the input and ruin the groupby
elf_identifier = corrected_values.withColumn("idx", monotonically_increasing_id())
elf_identifier = elf_identifier.withColumn("is_empty", when(col("value") == "", 1).otherwise(0))

# Useful to test if specific values are in the list or not
# elf_identifier.filter(col('value') == 5694).show()

# Calculate the cumulative sum of the boolean column to create group IDs
window_spec = Window.orderBy("idx")
elf_identifier = elf_identifier.withColumn("group_id", sum("is_empty").over(window_spec))

# Increment the group ID by 1 so that the first group has ID 1
elf_identifier = elf_identifier.withColumn("group_id", col("group_id") + 1)

# Identifies specific group IDs to test
# elf_identifier.filter(col('group_id') == 130).show()

sum_calories_per_elf = elf_identifier.groupBy(col('group_id')).sum('correct_value')

# corrected_values.printSchema()
sum_calories_per_elf.orderBy(col('sum(correct_value)').desc()).show()

"""
Up to this point the first part of the challenge is completed
The second part has the question: How many Calories are those Elves (top 3) carrying in total?
"""

sum_calories_per_elf = sum_calories_per_elf.orderBy(col('sum(correct_value)').desc()).limit(3)

top3_elves = sum_calories_per_elf
top3_elves = top3_elves.groupBy().sum('sum(correct_value)')
top3_elves.show()
