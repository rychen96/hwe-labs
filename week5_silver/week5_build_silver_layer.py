import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
class_name = "fall-2024"
handle = "rbrandt"

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week5Lab") \
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

bronze_reviews_schema = StructType([
    StructField("marketplace", StringType(), nullable=True)
    ,StructField("customer_id", StringType(), nullable=True)
    ,StructField("review_id", StringType(), nullable=True)
    ,StructField("product_id", StringType(), nullable=True)
    ,StructField("product_parent", StringType(), nullable=True)
    ,StructField("product_title", StringType(), nullable=True)
    ,StructField("product_category", StringType(), nullable=True)
    ,StructField("star_rating", StringType(), nullable=True)
    ,StructField("helpful_votes", StringType(), nullable=True)
    ,StructField("total_votes", StringType(), nullable=True)
    ,StructField("vine", StringType(), nullable=True)
    ,StructField("verified_purchase", StringType(), nullable=True)
    ,StructField("review_headline", StringType(), nullable=True)
    ,StructField("review_body", StringType(), nullable=True)
    ,StructField("purchase_date", StringType(), nullable=True)
    ,StructField("review_timestamp", TimestampType(), nullable=True)
])

#week 4, streaming
bronze_reviews = spark.readStream.schema(bronze_reviews_schema).parquet(f"s3a://hwe-{class_name}/{handle}/bronze/reviews")

#week 3, static
bronze_customers = spark.read.parquet(f"s3a://hwe-{class_name}/{handle}/bronze/customers")

#join two above
silver_data = bronze_reviews.join(bronze_customers, on="review_id", how="inner")


streaming_query = silver_data \
  .writeStream \
  .outputMode("append") \
  .format("parquet") \
  .option("path", f"s3a://hwe-{class_name}/{handle}/silver/reviews") \
  .option("checkpointLocation", "C:/Users/ryche/Documents/data_engineering/tmp2/kafka-checkpoint-silver")

streaming_query.start().awaitTermination()

## Stop the SparkSession
spark.stop()
