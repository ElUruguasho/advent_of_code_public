

"""
A, Y
B, X
C, Z
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import findspark
findspark.init()


spark = SparkSession.builder.getOrCreate()

data = [("A", "Y"), ("B", "X"), ("C", "Z")]

# Rename 2 columns at the same time
strategy = spark.createDataFrame(data)
strategy_renamedCol = strategy.withColumnsRenamed({"_1": "Opponent_Play", "_2": "My_Play"})

# Create a prefix of the fields provided by the advent exercise
strategy_descriptions = [col(col_name).alias("Advent_" + col_name) for col_name in strategy_renamedCol.columns]
prefixed_df = strategy_renamedCol.select(*strategy_descriptions)
# prefixed_df.show()

# Create 2 new fields that show the description of what each letter means
# A="Rock" , B="Paper", C="Scissors" / Y="Paper", X="Rock", Z="Scissors"
# Items have values: "Rock"= 1, "Paper"= 2, "Scissors"= 3
# Outcomes also have values: "Lost"= 0, "Draw"= 3, "Win"= 6
# A<Y, B<Z, C<X
# A=X, B=Y, C=Z
# A>Z, B>X, C>Y

"""
Example with a UDF which I need to study more

mapping = {"A": "Rock", "B": "Paper", "C": "Scissors"}
def map_value(value):
    return mapping.get(value, "Unknown")
map_value_udf = udf(map_value, StringType())

descriptive_df = prefixed_df.withColumn("Oppo_strat", map_value_udf(col("Advent_Opponent_Play")))
descriptive_df.show()
"""
opponent_strat = (  when(col("Advent_Opponent_Play") == "A", "Rock")
                   .when(col("Advent_Opponent_Play") == "B", "Paper")
                   .when(col("Advent_Opponent_Play") == "C", "Scissors")
                  )

my_strat = ( when(col("Advent_My_Play") == "Y", "Paper")
            .when(col("Advent_My_Play") == "X", "Rock")
            .when(col("Advent_My_Play") == "Z", "Scissors")
            )

descriptive_df = prefixed_df.withColumn("Oppo_strat", opponent_strat)
descriptive_df = descriptive_df.withColumn("My_strat", my_strat)

oppo_strat_points = (  when(col("Advent_Opponent_Play") == "A", 1)
                 .when(col("Advent_Opponent_Play") == "B", 2)
                 .when(col("Advent_Opponent_Play") == "C", 3)
                )

my_strat_points = ( when(col("Advent_My_Play") == "X", 1)
                   .when(col("Advent_My_Play") == "Y", 2)
                   .when(col("Advent_My_Play") == "Z", 3)
                  )

descriptive_df = prefixed_df.withColumn("Oppo_strat_points", oppo_strat_points)
descriptive_df = descriptive_df.withColumn("My_strat_points", my_strat_points)


outcome_points = (when(col("My_strat_points") - col("Oppo_strat_points") == 0, 3)
                 .when(col("My_strat_points") - col("Oppo_strat_points") > 0, 6)
                 .when(col("My_strat_points") - col("Oppo_strat_points") < 0, 0)
                 .when(col("My_strat_points") - col("Oppo_strat_points") == 2, 0)
                 .when(col("My_strat_points") - col("Oppo_strat_points") == -2, 6)
                 )

descriptive_df = descriptive_df.withColumn("match_points", outcome_points)
descriptive_df = descriptive_df.withColumn("total_points", col("match_points") + col("My_strat_points"))

descriptive_df = descriptive_df.groupBy().sum("total_points")
descriptive_df.show()

