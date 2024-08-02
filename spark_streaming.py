import pyspark # type: ignore
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.type import StructType, StringType, StructField # type: ignore
from pyspark.sql.functions import from_json, col, sum as _sum # type: ignore

if __name__ == '__main__':
    # print(pyspark.__version__)
    # init spark session 
    spark = (SparkSession.builder.appName("RealtimeVotingEngineering")
             .config('spark.jars.package', 'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1') # spark kafka integration
             .config('spark.jars', 'D:\rtvote\postgresql-42.7.3.jar') # postgresql driver
             .config('spark.sql.adaptive.enable', 'false') # adaptive query execution is disabled
             .getOrCreate())
    
    vote_schema = StructType([
        StructField('voter_id', StringType(), True),
        StructField('candidate_id', StringType(), True),
        StructField('voting_time', StringType(), True),
        StructField('voter_name', StringType(), True),
        StructField('party_affiliation', StringType(), True),
        StructField('biography', StringType(), True),
        StructField('campaign_platform', StringType(), True),
        StructField('photo_url', StringType(), True),
        StructField('candidate_name', StringType(), True),
        StructField('date_of_birth', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('nationality', StringType(), True),
        StructField('registration_number', StringType(), True),
        StructField('address', StructType([
            StructField('street', StringType(), True),
            StructField('city', StringType(), True),
            StructField('state', StringType(), True),
            StructField('country', StringType(), True),
            StructField('postcode', StringType(), True)
        ]), True),
        StructField('voter_id', StringType(), True),
        StructField('phone_number', StringType(), True),
        StructField('picture', StringType(), True),
        StructField('registered_age', StringType(), True),
        StructField('vote', StringType(), True)
    ])

    votes_df = (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'localhost:9092')
                .option('subscribe', 'votes_topic')
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col('value'), vote_schema).alias('data'))
                .select('data.*')
                )

    