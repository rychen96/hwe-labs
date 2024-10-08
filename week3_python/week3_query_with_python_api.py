import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col
from pyspark.sql.functions import current_timestamp

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
class_name = "fall-2024"
handle = "rbrandt"

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week3Lab") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .master('local[*]') \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

### Questions

# Remember, this week we are using the Spark DataFrame API (and last week was the Spark SQL API).

#Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe.
#You will use the "reviews" dataframe defined here to answer all the questions below...
# reviews = spark.read.csv("resources/reviews.tsv.gz", sep='\t', header=True, inferSchema=True)
reviews = spark.read.csv("resources/reviews.tsv.gz", sep='\t', header=True)
# reviews.show()

#Question 2: Display the schema of the dataframe.
print(f"\nQ2: The reviews dataframe has been made with the following schema:\n")
reviews.printSchema()

#Question 3: How many records are in the dataframe? 
#Store this number in a variable named "reviews_count".
reviews_count = reviews.count()
print(f"\nQ3: The number of reviews is {reviews_count}.\n")

#Question 4: Print the first 5 rows of the dataframe. 
#Some of the columns are long - print the entire record, regardless of length.
print(f"\nQ4: Below are the first 5 rows of the dataframe:\n")
reviews.show(n=5, truncate=False)

#Question 5: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
#Look at the first 50 rows of that dataframe. 
#Which value appears to be the most common?
reviews_pc = reviews.select('product_category')
# reviews_pc.show(n=50)
print(f"\nQ5: Based on the first 50 rows of reviews_pc, the most common product category is Digital Video Games.\n")

#Question 6: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
#What is the product title for that review? How many helpful votes did it have?
print(f"\nQ6: The most helpful review was the following:\n")
reviews.select('product_title', 'helpful_votes').orderBy(reviews['helpful_votes'].desc()).limit(1).show()

#Question 7: How many reviews have a 5 star rating?
five_star = reviews.filter(reviews['star_rating'] == '5').count()
print(f"\nQ7: There were {five_star} reviews with a 5-star rating.\n")

#Question 8: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
#Create a new dataframe with just those 3 columns, except cast them as "int"s.
#Look at 10 rows from this dataframe.
reviews_cast = reviews.withColumn("star_rating", col("star_rating").cast("Integer")) \
    .withColumn("helpful_votes", col("helpful_votes").cast("Integer")) \
    .withColumn("total_votes", col("total_votes").cast("Integer"))
print(f"\nQ8: Cast rows:")
reviews_cast.printSchema()

#Question 9: Find the date with the most purchases.
#Print the date and total count of the date with the most purchases    
date_with_most_purchases = reviews.withColumn("purchase_date", col("purchase_date").cast("Date")) \
    .groupBy('purchase_date').count() \
    .orderBy(col('count').desc())
print(f"\nQ9: The date with the most purchases is:\n")
date_with_most_purchases.limit(1).show()

#Question 10: Add a column to the dataframe named "review_timestamp", representing the current time on your computer. 
#Hint: Check the documentation for a function that can help: https://spark.apache.org/docs/3.1.3/api/python/reference/pyspark.sql.html#functions
#Print the schema and inspect a few rows of data to make sure the data is correctly populated.
reviews = reviews.withColumn('review_timestamp', current_timestamp())
print(f"\nQ10: The column 'review_timestamp' has been added to the reviews dataframe (Sample row below).\n")
reviews.show(n=1, truncate=True)
reviews.printSchema()

#Question 11: Write the dataframe with load timestamp to s3a://hwe-$CLASS/$HANDLE/bronze/reviews_static in Parquet format.
#Make sure to write it using overwrite mode: append will keep appending duplicates, which will cause problems in later labs...
reviews.write.mode("overwrite").parquet(f"s3a://hwe-{class_name}/{handle}/bronze/reviews_static")
print(f"\nQ11: Wrote reviews_static! Has the following schema:\n")
reviews.printSchema()

#Question 12: Read the tab separated file named "resources/customers.tsv.gz" into a dataframe
#Write to S3 under s3a://hwe-$CLASS/$HANDLE/bronze/customers
#Make sure to write it using overwrite mode: append will keep appending duplicates, which will cause problems in later labs...
#There are no questions to answer about this data set right now, but you will use it in a later lab...
reviews_original = spark.read.csv("resources/reviews.tsv.gz", sep='\t', header=True)
reviews_original.write.mode("overwrite").parquet(f"s3a://hwe-{class_name}/{handle}/bronze/customers")
print(f"\nQ12: Wrote customers! Has the following schema:\n")
reviews_original.printSchema()

# Stop the SparkSession
spark.stop()