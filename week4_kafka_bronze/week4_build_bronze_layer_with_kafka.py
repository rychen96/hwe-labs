import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, split

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

def getScramAuthString(username, password):
  return f"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username="{username}"
   password="{password}";
  """

# Define the Kafka broker and topic to read from
kafka_bootstrap_servers = os.environ.get("HWE_BOOTSTRAP")
username = os.environ.get("HWE_USERNAME")
password = os.environ.get("HWE_PASSWORD")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
class_name = "fall-2024"
handle = "rbrandt"
kafka_topic = "reviews"

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week4Lab") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

# Read data from Kafka using the DataFrame API
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "10") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", getScramAuthString(username, password)) \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .selectExpr("SPLIT(value, '\t') as split_values") \
    .selectExpr( 
    "split_values[0] as marketplace",
    "split_values[1] as customer_id",
    "split_values[2] as review_id",
    "split_values[3] as product_id",
    "split_values[4] as product_parent",
    "split_values[5] as product_title",
    "split_values[6] as product_category",
    "split_values[7] as star_rating",
    "split_values[8] as helpful_votes",
    "split_values[9] as total_votes", 
    "split_values[10] as vine",
    "split_values[11] as verified_purchase",
    "split_values[12] as review_headline",
    "split_values[13] as purchase_date" 
) \
    .withColumn('review_timestamp', current_timestamp())

  #  * append a column to the data named `review_timestamp` which is set to the current_timestamp


  #  * write that data as Parquet files to S3 under `s3a://hwe-$CLASS/$HANDLE/bronze/reviews` using append mode and a checkpoint location of `/tmp/kafka-checkpoint`
query = df \
  .writeStream \
  .outputMode("append") \
  .format("parquet") \
  .option("path", f"s3a://hwe-{class_name}/{handle}/bronze/reviews") \
  .option("checkpointLocation", "C:/Users/ryche/Documents/data_engineering/tmp/kafka-checkpoint") \
  .start()

# Process the received data. Note, turning off truncate lets us view the data. Also note that "show" does not work with streaming queries.
# The below query is to troubleshoot in console.
# query = df \
#   .writeStream \
#   .format("console") \
#   .outputMode("append") \
#   .option("checkpointLocation", "C:/Users/ryche/Documents/data_engineering/tmp/kafka-checkpoint") \
#   .start()
  # .option("truncate", "false") \ include this in the query to see the full message

# Wait for the streaming query to finish
query.awaitTermination()

# Stop the SparkSession
spark.stop()