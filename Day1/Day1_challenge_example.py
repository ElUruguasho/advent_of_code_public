"""
Calories per elf

[("1st elf", 1000, 2000, 3000), 
("2nd elf", 4000),
("3rd elf", 5000, 6000),
("4th elf", 7000, 8000, 9000),
("5th elf", 10000)]
"""


from pyspark.sql import SparkSession
import findspark
findspark.init()

spark = SparkSession.builder.getOrCreate()

# data structure for the dataframe
data = [("1st elf", 1000, 2000, 3000), 
        ("2nd elf", 4000, None, None),
        ("3rd elf", 5000, 6000, None),
        ("4th elf", 7000, 8000, 9000),
        ("5th elf", 10000, None, None)]

# creates dataframe and displays it
elf_table = spark.createDataFrame(data)

# unpivot dataframe in order to sum the calories grouped by elfs
elf_table = elf_table.unpivot("_1", ["_2", "_3", "_4"], "Calorie_ID", "Calories")
elf_table = elf_table.withColumnRenamed("_1", "Elf")
# elf_table.select("Elf", "Calories").show()

# Create a groupby with the sum of values
elf_table = elf_table.groupBy("Elf").sum("Calories")
elf_table = elf_table.orderBy("sum(Calories)", ascending = False).limit(1).show()