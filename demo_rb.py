from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Renee") \
    .master("local[1]") \
    .getOrCreate()
    
version = spark.version
print(f"\nI've started a Spark cluster running Spark {version}!\n")

scores_data = spark.read.csv("resources/video_game_scores.tsv", sep=",", header=True)
scores_data.printSchema()
scores_data.show(n=4, truncate=False)

# scores_data.write.json("resources/video_game_scores_json")
