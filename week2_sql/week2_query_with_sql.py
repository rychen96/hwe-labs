from pyspark.sql import SparkSession
from pyspark.sql import functions

### Setup: Create a SparkSession
spark = SparkSession.builder \
    .appName("Renee") \
    .master("local[1]") \
    .getOrCreate()
    
version = spark.version
print(f"\nI've started a Spark cluster running Spark {version}!\n")

# For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

### Questions

# Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe. Call it "reviews".
reviews = spark.read.csv("resources/reviews.tsv.gz", sep="\t", header=True)
print(f"\nQ1: The reviews dataframe has been made with the following schema:\n")
reviews.printSchema()
# reviews.show(n=4, truncate=True)

# Question 2: Create a virtual view on top of the reviews dataframe, so that we can query it with Spark SQL.
reviews.createOrReplaceTempView("reviews_view")
print(f"\nQ2: The virtual view on top of the reviews dataframe 'reviews_view' has been made.\n")

# Question 3: Add a column to the dataframe named "review_timestamp", representing the current time on your computer. 
reviews_with_tsp = reviews.withColumn('review_timestamp', functions.current_timestamp())
print(f"\nQ3: The column 'review_timestamp' has been added to the reviews dataframe (Sample row below).\n")
reviews_with_tsp.show(n=1, truncate=True)

# Question 4: How many records are in the reviews dataframe?
row_count = spark.sql("SELECT COUNT(*) AS count from reviews_view").collect()[0]["count"]
print(f"\nQ4: The number of rows in the reviews dataframe is {row_count}.\n")

# Question 5: Print the first 5 rows of the dataframe. 
# Some of the columns are long - print the entire record, regardless of length.
print(f"\nQ5: The column 'review_timestamp' has been added to the reviews dataframe (Sample row below).\n")
reviews_with_tsp.show(n=5, truncate=False)

# Question 6: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
# Look at the first 50 rows of that dataframe. 
# Which value appears to be the most common?
product_df = spark.sql("SELECT product_category FROM reviews_view")
# product_df.show(50)
print(f"\nQ6: based on the top 50 rows, the most common product category value is Digital Video Games.\n")

# Question 7: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
# What is the product title for that review? How many helpful votes did it have?
# TODO: cast helpful_votes from string to int and order by helpful_votes descending, then select the first row
most_helpful_review = spark.sql("""SELECT CAST(helpful_votes AS INT) AS helpful_votes, product_title 
                                FROM reviews_view
                                ORDER BY helpful_votes DESC
                                LIMIT 1""").collect()
most_helpful_review_votes = most_helpful_review[0]['helpful_votes']
most_helpful_review_title = most_helpful_review[0]['product_title']
print(f"\nQ7: The most helpful review was for the product {most_helpful_review_title} ({most_helpful_review_votes} helpful_votes).\n")

# Question 8: How many reviews exist in the dataframe with a 5 star rating?
five_star_count = spark.sql("SELECT COUNT(*) as five_star_count FROM reviews_view WHERE star_rating = '5'").collect()[0]["five_star_count"]
print(f"\nQ8: The number of reviews with a 5 star rating is {five_star_count}.\n")
                      
# Question 9: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
# Create a new dataframe with just those 3 columns, except cast them as "int"s.
# Look at 10 rows from this dataframe.
print(f"\nQ9: The following rows have been cast from STRING to INT:\n")
reviews_cast = spark.sql("""SELECT CAST(star_rating AS INT) as star_rating, 
                         CAST(helpful_votes AS INT) as helpful_votes, 
                         CAST(total_votes AS INT) as total_votes
                         FROM reviews_view 
                         """).show(10)

# Question 10: Find the date (purchase_date) with the most purchases.
# Print the date and total count of the date which had the most purchases.
print(f"\nQ10: The following date has the most purchases:\n")
busiest_date = spark.sql("""SELECT CAST(purchase_date AS DATE) as purchase_date, COUNT(*) as count
                         FROM reviews_view
                         GROUP BY purchase_date
                         ORDER BY count DESC""").show(1)

##Question 11: Write the dataframe from Question 3 to your drive in JSON format.
##Feel free to pick any directory on your computer.
##Use overwrite mode.
print(f"\nQ11: Writing reviews_with_tsp dataframe to local JSON...")
reviews_with_tsp.write.mode("overwrite").json("week2_sql/reviews_with_tsp_json")
print(f"\nOverwrite complete.\n")

### Teardown
# Stop the SparkSession
print(f"\nAll done! Closing spark session.\n")
spark.stop()