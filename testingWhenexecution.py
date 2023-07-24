from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, monotonically_increasing_id, udf
from pyspark.sql.types import IntegerType
import timeit
import findspark
findspark.init()


spark = SparkSession.builder.getOrCreate()

data = spark.read.format("text").load("C:/Python_projects/advent_of_code/Day2/Day2_input.txt")

# Split field "value" from input in two separate fields so we can apply logic to it
data = data.withColumn("idx", monotonically_increasing_id()).orderBy("idx")
split_field = data.withColumn("_1", split(data['value'], ''))
additional_fields = (split_field.withColumn("_2", split_field["_1"].getItem(0))
                               .withColumn("_3", split_field["_1"].getItem(2)))

# Rename 2 columns at the same time
strategy_renamedCol = additional_fields.withColumnsRenamed({"_2": "Opponent_Play", "_3": "My_Play"})

# Create a prefix of the fields provided by the advent exercise
strategy_descriptions = [col(col_name).alias("Advent_" + col_name) for col_name in strategy_renamedCol.columns]
prefixed_df = strategy_renamedCol.select(*strategy_descriptions)

# provides description to opponent strategy
opponent_strat = (  when(col("Advent_Opponent_Play") == "A", "Rock")
                   .when(col("Advent_Opponent_Play") == "B", "Paper")
                   .when(col("Advent_Opponent_Play") == "C", "Scissors")
                  )
# provides description to my chosen strategy
my_strat = ( when(col("Advent_My_Play") == "Y", "Paper")
            .when(col("Advent_My_Play") == "X", "Rock")
            .when(col("Advent_My_Play") == "Z", "Scissors")
            )
# creates the 2 new fields with the descriptions
descriptive_df = prefixed_df.withColumn("Oppo_strat", opponent_strat)
descriptive_df = descriptive_df.withColumn("My_strat", my_strat)

# provides the points for the chosen strategy of my opponent
oppo_strat_points = (  when(col("Advent_Opponent_Play") == "A", 1)
                      .when(col("Advent_Opponent_Play") == "B", 2)
                      .when(col("Advent_Opponent_Play") == "C", 3)
                    )

# provides the points for my chosen strategy
my_strat_points = ( when(col("Advent_My_Play") == "X", 1)
                   .when(col("Advent_My_Play") == "Y", 2)
                   .when(col("Advent_My_Play") == "Z", 3)
                  )
# creates the 2 additional fields to display the values for each strategy
descriptive_df = descriptive_df.withColumn("Oppo_strat_points", oppo_strat_points)
descriptive_df = descriptive_df.withColumn("My_strat_points", my_strat_points)

# The order of these conditions is relevant to the final corrrect outcome
outcome_points = (when(col("My_strat_points") - col("Oppo_strat_points") == 0, 3)
                 .when(col("My_strat_points") - col("Oppo_strat_points") == 2, 0)
                 .when(col("My_strat_points") - col("Oppo_strat_points") > 0, 6)
                 .when(col("My_strat_points") - col("Oppo_strat_points") == -2, 6)
                 .when(col("My_strat_points") - col("Oppo_strat_points") < 0, 0)
                 )

descriptive_df = descriptive_df.withColumn("match_points", outcome_points)
descriptive_df = descriptive_df.withColumn("total_points", col("match_points") + col("My_strat_points"))

# descriptive_df.show(50)

total_descriptive_df = descriptive_df.groupBy().sum("total_points")
# total_descriptive_df.show()
# 11906

"""
Up to this point the part 1 of the challenge is completed
"""
# new strategy field with the conditions needed to follow in the second time
new_strat_condition = (when(col("Advent_My_Play") == "X", "lose")
                      .when(col("Advent_My_Play") == "Y", "draw")
                      .when(col("Advent_My_Play") == "Z", "win"))

# creation of the new field for the new strategy
new_strat = descriptive_df.withColumn("needed_result", new_strat_condition)

# Conditions for the new strategy points
new_play_strat = (when(col("needed_result") == "draw", 3)
                .when(col("needed_result") == "lose", 0)
                .when(col("needed_result") == "win", 6))

new_play_points = new_strat.withColumn("new_points", new_play_strat)

point_dif = new_play_points.withColumn("point_dif", col("My_strat_points") - col("Oppo_strat_points"))

"""
Below is the original solution I reached but it was super ugly as the rest of the script
"""
my_new_strat = (when(col("needed_result") == "draw", col("Oppo_strat_points"))
                .when((col("needed_result") == "win") & (col("Oppo_strat_points") == 3), 1)
                .when((col("needed_result") == "win") & (col("Oppo_strat_points") == 1), 2)
                .when((col("needed_result") == "win") & (col("Oppo_strat_points") > 0), col("My_strat_points"))
                .when((col("needed_result") == "lose") & (col("point_dif") == -2), 2)
                .when((col("needed_result") == "lose") & (col("Oppo_strat_points") == 1), 3)
                .when((col("needed_result") == "lose") & (col("point_dif") < 0), col("My_strat_points"))
                .when((col("needed_result") == "lose") & (col("point_dif") == 0), col("My_strat_points")+1)
               ).otherwise(0)

my_new_strat_field = point_dif.withColumn("New_my_play", my_new_strat)

when_time = timeit.timeit(
    "my_new_strat_field.count()",
    globals=globals(),
    number=1
)
print(f"Chained when conditions execution time: {when_time:.2f} seconds")
# Chained when conditions execution time: 1.33 seconds

"""
Below is a more sophisticated solution provided by phind (lines 128 to 150). The question now is, which one has better performance with sizeable datasets?

@udf(IntegerType())
def calculate_my_new_strat(needed_result, oppo_strat_points, my_strat_points, point_dif):
    if needed_result == "draw":
        return oppo_strat_points
    elif needed_result == "win":
        if oppo_strat_points == 3:
            return 1
        elif oppo_strat_points == 1:
            return 2
        elif oppo_strat_points > 0:
            return my_strat_points
    elif needed_result == "lose":
        if point_dif == -2:
            return 2
        elif oppo_strat_points == 1:
            return 3
        elif point_dif < 0:
            return my_strat_points
        elif point_dif == 0:
            return my_strat_points + 1
    else:
        return 0
my_new_strat_field = point_dif.withColumn("New_my_play", calculate_my_new_strat(col("needed_result"), col("Oppo_strat_points"), col("My_strat_points"), col("point_dif")))

new_total_points = my_new_strat_field.withColumn("new_total_points", col("New_my_play") + col("new_points"))

udf_time = timeit.timeit(
    "my_new_strat_field.withColumn('result', calculate_my_new_strat(my_new_strat_field['needed_result'], my_new_strat_field['oppo_strat_points'], my_new_strat_field['my_strat_points'], my_new_strat_field['point_dif'])).count()",
    globals=globals(),
    number=1)

print(f"UDF execution time: {udf_time:.2f} seconds")
# UDF execution time: 1.32 seconds
"""
# new_total_points.show(50)
# new_total_points.groupBy().sum("new_total_points").show()


