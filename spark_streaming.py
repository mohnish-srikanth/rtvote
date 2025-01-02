import pyspark # type: ignore
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, TimestampType # type: ignore
from pyspark.sql.functions import from_json, col, sum as _sum # type: ignore
import os
import sys

if __name__ == "__main__":
    # print(pyspark.__version__)
    # init spark session 
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable    
    spark = (SparkSession.builder.appName("RealtimeVotingEngineering") 
            .config("spark.master", "local[*]")  # Ensure local execution
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3')
            .config("spark.jars", "D:\\rtvote\\postgresql-42.7.3.jar")
            .config("spark.sql.adaptive.enable", "false")
            .config("spark.driver.host", "localhost")  # Explicitly set driver host
            .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
            .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
            .config("spark.driver.extraClassPath", r"C:\hadoop\bin")
            .config("spark.executor.extraClassPath", r"C:\hadoop\bin")
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .config("spark.python.worker.reuse", "true")
            .config("spark.pyspark.python", "python")  # Specify the Python interpreter
            .config("spark.pyspark.driver.python", "python")  # Specify the Python interpreter for the driver
            .getOrCreate())
    # spark.sparkContext.setLogLevel("ERROR")

    vote_schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postcode", StringType(), True)
        ]), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True)
    ])

    # raw_df = spark.readStream \
    # .format("kafka") \
    # .option("kafka.bootstrap.servers", "localhost:9092") \
    # .option("subscribe", "votes_topic") \
    # .option("startingOffsets", "earliest") \
    # .load()

    # raw_df.selectExpr("CAST(value AS STRING)").writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .start() \
    #     .awaitTermination()

    # parsed_df = raw_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), vote_schema).alias("data"))
    # parsed_df.writeStream \
    # .format("console") \
    # .outputMode("append") \
    # .start() \
    # .awaitTermination()

    votes_df = (spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "votes_topic") \
                .option("startingOffsets", "earliest") \
                .load() \
                .selectExpr("CAST(value AS STRING)")\
                .select(from_json(col("value"), vote_schema).alias("data"))\
                .select("data.*")
                )

    # data preprocessing typecasting
    votes_df = votes_df.withColumn("voting_time", col("voting_time").cast(TimestampType())) \
                       .withColumn("vote", col("vote").cast(IntegerType()))
    enriched_votes_df = votes_df.withWatermark("voting_time", "1 minute")

    # aggregate vote per candidate and turnout by location
    votes_per_candidate = enriched_votes_df.groupBy("candidate_id", "candidate_name", "party_affiliation", "photo_url").agg(_sum("vote").alias("total_votes"))
    turnout_by_location = enriched_votes_df.groupBy("address.state").count().alias("total_votes")

    # test_df = spark.createDataFrame([
    # {"candidate_id": "1", "candidate_name": "John Doe", "party_affiliation": "Independent", "photo_url": "url", "total_votes": 100}
    # ])

    # test_df.selectExpr("to_json(struct(*)) AS value") \
    #     .write \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("topic", "aggregated_votes_per_candidate") \
    #     .save()

    votes_per_candidate_to_kafka = votes_per_candidate.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_votes_per_candidate") \
        .option("checkpointLocation", "D:/rtvote/venv/checkpoints/checkpoint1") \
        .outputMode("update") \
        .start()

    turnout_by_location_to_kafka = turnout_by_location.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "aggregated_turnout_by_location") \
        .option("checkpointLocation", "D:/rtvote/venv/checkpoints/checkpoint2") \
        .outputMode("update") \
        .start()
    # waiting for streaming queries termination
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_location_to_kafka.awaitTermination()