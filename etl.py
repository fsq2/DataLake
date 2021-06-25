import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data =input_data + "song_data/*/*/*"

    
    # read song data file
    df = spark.read.format("json").load(song_data)

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT DISTINCT song_id,
                                   LTRIM(RTRIM(title)) AS title,
                                   artist_id,
                                   IF(year=0,null,year) AS year,
                                   duration
                            FROM songs
                           """).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs_table")


# extract columns to create artists table
 artists_table = spark.sql("""
                                SELECT DISTINCT
                                       artist_id,
                                       artist_name,
                                       IF(artist_location='' OR artist_location='None',null,artist_location) AS
                                       artist_location,
                                       artist_latitude,
                                       artist_longitude
                                FROM songs
                             """).dropDuplicates()
    
    
# write artists table to parquet files
    artists_table
     artists_table.write.parquet(output_data + "artists_table")


def process_log_data(spark, input_data, output_data):
     """
    Description: This function can be used to read the log data in the
        filepath (bucket/log_data) to get the info to populate the
        user, time and song dim tables as well as the songplays fact table.
    Parameters:
        spark
        input_path
        output_path
            
    Returns:
        None
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*"

    # read log data file
     df = spark.read.format("json").load(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT qry.userid,
                                   qry.firstname,
                                   qry.lastname,
                                   qry.gender,
                                   qry.level
                              FROM (
                                    SELECT start_time,
                                           userid,
                                           firstname,
                                           lastname,
                                           gender,
                                           level,
                                           RANK() OVER (PARTITION BY userid ORDER BY start_time DESC) AS rank
                                      FROM log_data
                                   ) AS qry
                            WHERE qry.rank = 1
                           """).dropDuplicates()
     
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users_table")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = df.select("start_time",
                           hour("start_time").alias('hour'),
                           dayofmonth("start_time").alias('day'),
                           weekofyear("start_time").alias('week'),
                           month("start_time").alias('month'),
                           year("start_time").alias('year'),
                           date_format("start_time","u").alias('weekday')
                          ).distinct()
    
    # write time table to parquet files partitioned by year and month
        time_table.write.partitionBy("year", "month").parquet(output_data + "time_table")


    # read in song data to use for songplays table
       song_df = spark.read.parquet("s3a://project4dend/songs_table")
    song_df.createOrReplaceTempView("songs_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT l.start_time,
                                       t.year,
                                       t.month,
                                       l.userid,
                                       l.level,
                                       q.song_id,
                                       q.artist_id,
                                       l.sessionid,
                                       l.location,
                                       l.useragent
                                  FROM log_data l
                                  JOIN time_table t ON (l.start_time = t.start_time)
                                 LEFT JOIN (
                                           SELECT s.song_id,
                                                  s.title,
                                                  a.artist_id,
                                                  a.artist_name
                                             FROM songs_table s
                                             JOIN artists_table a ON (s.artist_id = a.artist_id)
                                          ) AS q ON (l.song = q.title AND l.artist = q.artist_name)
                               """).dropDuplicates()
    


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data + "songplays_table")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://project4dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
