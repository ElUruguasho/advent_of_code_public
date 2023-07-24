from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import length, col, split, substring, expr, array_except, create_map, lit, row_number, ceil, monotonically_increasing_id
import findspark
findspark.init()


spark = SparkSession.builder.getOrCreate()

#data = spark.read.format("text").load("C:/Python_projects/advent_of_code/Day3/Day3_input.txt")

data = ['vJrwpWtwJgWrhcsFMMfFFhFp', 'jqHRNqRjqzjGDLGLrsFMfFZSrLrFZsSL', 'PmmdzqPrVvPwwTWBwg', 'wMqvLMZHhHMvwLHjbvcjnnSBnvTQFn', 'ttgJtRGJQctTZtZT', 'CrZsJsPPZsGzwwsLwLmpwMDw']

data = spark.createDataFrame(data, StringType())

# Splits each string into a list of each letter
string_length = data.withColumn('string_list', split(data['value'], ''))
# string_length.show()

# counts the length of the list
string_length = string_length.withColumn('string_length', length(string_length['value']))
# string_length.show()

half_string = string_length.withColumn('half_string_number', (col('string_length')/2).cast(IntegerType()))
# half_string.show()

# In a weird turn of events, the substring function needs to pass an actual string as a positional value, I could only achieve it by wrapping it in an expr function
# I also one position for the second half substring following the example provided
string_split_2 = half_string.withColumn('first_half_string', expr("substring(value, 1, half_string_number)"))
string_split_2 = string_split_2.withColumn('second_half_string', expr("substring(value, half_string_number+1)"))
# string_split_2.show()

list_of_strings1 = string_split_2.withColumn('list_first_half', split(string_split_2['first_half_string'], ''))
list_of_strings2 = list_of_strings1.withColumn('list_second_half', split(string_split_2['second_half_string'], ''))
# list_of_strings2.show()

# The array except will only give the items that are not part of the 2nd list
comparison = list_of_strings2.withColumn('unique_values_first_list', array_except(col('list_first_half'), (col('list_second_half'))))
# Now we repeat the comparison to its own list and will get thos values that are repeated in both lists
comparison = comparison.withColumn('rucksack_duplicates', array_except(col('list_first_half'), (col('unique_values_first_list'))))

# Now I want to turn the array value from "rucksack duplicates" into a string
comparison = comparison.withColumn('duplicate_values', comparison['rucksack_duplicates'].getItem(0))
# comparison.show()

"""
Now to the dictionary values and the sum of piriorities
Priority values:
a to z = 1 to 26
A to Z = 27 to 52

The enumerate() function is used to loop through the alphabet string and generate index-value pairs. 
The index is incremented by 1 to get the corresponding value for each letter. 
The dictionary comprehension creates the final dictionary with letter keys and their respective numeric values.
"""
alphabet = 'abcdefghijklmnopqrstuvwxyz'
alphabet_dict = {letter: index + 1 for index, letter in enumerate(alphabet)}
alphabet_upper = {letter.upper(): index + 27 for index, letter in enumerate(alphabet)}

"""
First, we create a separate dictionary for the uppercase letters with values incrementing from 27 onwards. 
Then, we use the update() method to merge the uppercase alphabet dictionary with the original lowercase alphabet dictionary.
"""
alphabet_dict.update(alphabet_upper)
# print(alphabet_dict)

# List comprehension to iterate through the dictionary and 
map_col = create_map([lit(x) for i in alphabet_dict.items() for x in i])
priorities = comparison.withColumn('priority_value', map_col[col('duplicate_values')])
# priorities.show()

total_priority = priorities.groupBy().sum('priority_value')
# total_priority.show()

# Important to have this id so the "values" are not reordered when doing the window orderby
id_to_avoid_scramble = priorities.withColumn('idx', monotonically_increasing_id())

row_number_window = Window.orderBy('idx')
# creates the row number that is going to be used to divide by 3 to get the group of three rows
new_badges = id_to_avoid_scramble.withColumn("row_number", row_number().over(row_number_window))
# uses ceiling value to get the correct output
new_badges = new_badges.withColumn('group_id', ceil(col('row_number')/3))
new_badges = new_badges.drop('string_length', 'first_half_string', 'second_half_string', 'rucksack_duplicates')
new_badges.show()

result = new_badges.groupBy('group_id').agg(expr("aggregate(collect_list(string_list), collect_list(string_list)[0], (acc, x) -> array_intersect(acc, x)) as intersection"))

result.show()
