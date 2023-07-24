from pyspark.sql import SparkSession
from pyspark.sql.functions import split, trim, expr, col, regexp_replace, monotonically_increasing_id, slice, when, udf
from pyspark.sql.types import IntegerType, StringType
import findspark

findspark.init()

spark = SparkSession.builder.getOrCreate()

# data = [2-4, 6-8, 2-3, 4-5, 5-7, 7-9, 2-8, 3-7, 6-6, 4-6, 2-6, 4-8]
data = spark.read.format("text").load("Day4_input.txt")

# data = data.limit(75)

# change the - for a , so it's easier to handle the array
data = data.withColumn('value_with_comma', regexp_replace('value', "-", ","))
# data.show()

# splitting the field will create an array with all the values
split_sections = data.withColumn('split_values',
                                 split(data['value_with_comma'], ","))

# transforms values into an integer to be able to perform substractions
to_int = split_sections.withColumn('int_values',
                                   expr(
                                       "transform(split_values, x -> int(x))"))

# id in case it's necessary to track the groups
added_id = to_int.withColumn('id', monotonically_increasing_id())

# getting each value of the array in order to operate on them as columns instead of array values
get_val_1 = added_id.withColumn(
    'val1', slice(col('int_values'), 1, 1).getItem(0))
get_val_2 = get_val_1.withColumn(
    'val2', slice(col('int_values'), 2, 1).getItem(0))
get_val_3 = get_val_2.withColumn(
    'val3', slice(col('int_values'), 3, 1).getItem(0))
get_val_4 = get_val_3.withColumn(
    'val4', slice(col('int_values'), 4, 1).getItem(0))

# drops fields for a more readable table
summary = get_val_4.drop('value', 'value_with_comma',
                         'split_values', 'int_values')

# doing substraction to identify which range is contained within each range
sub1 = summary.withColumn('first_interval', col('val1') - col('val3'))
sub2 = sub1.withColumn('second_interval', col('val2') - col('val4'))


"""
first_interval >=0 & second_interval >=0 then interval 2 included in interval 1
first_interval >=0 & second_interval <0 then interval 2 includes interval 1
"""

# Creating the when conditions from the above logic
included1 = sub2.withColumn('int2inint1', when((col('first_interval') > 0) &
                            (col('second_interval') > 0), None)
                            .when((col('first_interval') < 0) &
                            (col('second_interval') < 0), None)
                            .when((col('first_interval') >= 0) &
                            (col('second_interval') >= 0), "count1")
                            .when((col('first_interval') < 0) &
                            (col('second_interval') >= 0), "count1"))

included2 = included1.withColumn('int1inint2', when(
    (col('first_interval') >= 0) & (col('second_interval') < 0), "count1"))

consolidated = included2.withColumn('consolidated',
                                    when(col('int2inint1').isNotNull(),
                                         col('int2inint1'))
                                    .when(col('int1inint2').isNotNull(),
                                          col('int1inint2')))
# consolidated.show(80)
contained_pairs = consolidated.withColumn('contained', when(
    col('consolidated').isNotNull(), 1).otherwise(0))
# contained_pairs_result = contained_pairs.groupBy().sum('contained')

# contained_pairs_result.show()


"""
Here Starts part 2
"""


@udf(StringType())
def calculate_my_new_strat(val1, val2, val3, val4, first_interval, second_interval):
                            if (val4 > val2) & (val3 > val2):
                             return "KEK"
                            if (val4 < val1) & (val4 < val2):
                             return "KEK"                           
                            if val1 - val3 >= 0:
                             return "included"
                            if val2 - val4 <= 0:
                             return "included"
                            if first_interval < 0 & second_interval >= 0:
                             return "included" 
                            else:
                             return "kek"


udf_overlap = consolidated.withColumn('New_my_play',
                                      calculate_my_new_strat(col('val1'),
                                                             col('val2'),
                                                             col('val3'),
                                                             col('val4'),
                                                             col('first_interval'),
                                                             col('second_interval')))
udf_overlap.show(90)



udf_count = udf_overlap.groupBy('New_my_play').count()
udf_count.show()

