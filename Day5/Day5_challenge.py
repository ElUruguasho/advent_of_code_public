from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import findspark
findspark.init()


spark = SparkSession.builder.getOrCreate()

data = spark.read.format("text").load("Day5_input.txt")

data = data.limit(9)

split_df = data.withColumn('split', split('value', ''))
# get1 = split_df.withColumn('list1', col('split').getItem(1))
# get2 = get1.withColumn('list2', col('split').getItem(5))
# get3 = get2.withColumn('list3', col('split').getItem(9))
# get4 = get3.withColumn('list4', col('split').getItem(13))
# get5 = get4.withColumn('list5', col('split').getItem(17))
# get6 = get5.withColumn('list6', col('split').getItem(21))
# get7 = get6.withColumn('list7', col('split').getItem(25))
# get8 = get7.withColumn('list8', col('split').getItem(29))
# get9 = get8.withColumn('list9', col('split').getItem(33))

# Use a loop to create new columns with items separated by a 4-space difference
for i in range(1, 34, 4):
    split_df = split_df.withColumn(f'list{i}', col('split').getItem(i))

split_df = split_df.drop('value', 'split')    

split_df.show()
