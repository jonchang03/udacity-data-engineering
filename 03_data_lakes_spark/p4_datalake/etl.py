import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType, IntegerType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Retrieve existing Spark session or create a new one."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """This function loads song_data from S3, extracts columns from songs and artists table and writes them back to S3.

    Args:
        spark: Spark session
        input_data: Path to input datsets that reside in S3.
        output_data: Path where output parquet files will be written.

    Returns:
        None

    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", 
                            "title", 
                            "artist_id", 
                            "year", 
                            "duration")\
                    .drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
               .mode("overwrite") \
               .partitionBy("year", "artist_id") \
               .parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", 
                                  "artist_name as name", 
                                  "artist_location as location", 
                                  "artist_latitude as latitude", 
                                  "artist_longitude as longitude")\
                      .drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write \
                 .mode("overwrite") \
                 .parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """This function reads log event data from S3, extracts columns to create dimension tables: users, songs, artists, time, written out to parquet files.

    Args:
        spark: Spark session
        input_data: Path to input datsets that reside in S3.
        output_data: Path where output parquet files will be written.

    Returns:
        None

    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id",
                                "firstName as first_name",
                                "lastName as last_name",
                                "gender",
                                "level")
    
    # write users table to parquet files
    users_table.write \
               .mode("overwrite") \
               .parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x / 1000), IntegerType())
    df = df.withColumn("start_time", get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.utcfromtimestamp(x), TimestampType()) 
    df = df.withColumn('start_time', get_datetime('start_time'))
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour("start_time")) \
                   .withColumn("day", dayofmonth("start_time")) \
                   .withColumn("week", weekofyear("start_time")) \
                   .withColumn("month", month("start_time")) \
                   .withColumn("year", year("start_time")) \
                   .withColumn("weekday", dayofweek("start_time")) \
                   .select("start_time", "hour", "day", "week", "month", "year", "weekday").drop_duplicates()

    
    # write time table to parquet files partitioned by year and month
    time_table.write \
              .mode("overwrite") \
              .partitionBy("year", "month") \
              .parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song==song_df.title) & (df.artist==song_df.artist_name), how='left')\
                        .selectExpr("start_time", 
                                    "userId as user_id",
                                    "level",
                                    "song_id",
                                    "artist_id",
                                    "sessionId as session_id",
                                    "artist_location as location",
                                    "userAgent as user_agent")\
                        .withColumn("year", year("start_time"))\
                        .withColumn("month", month("start_time"))\
                        .drop_duplicates()\
                        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write \
                   .mode("overwrite") \
                   .partitionBy("year", "month") \
                   .parquet(output_data + "songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
