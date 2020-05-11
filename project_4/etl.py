import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('project_4/dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark


def process_song_data(spark, song_df, output_data):
    ## get filepath to song data file 
    #song_data = input_data + "song_data/*/*/*/*.json"
    #
    ## read song data file
    #song_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_df \
                  .select('song_id', 'title', 'artist_id', 'year', 'duration') \
                  .dropDuplicates() \
                  .dropna(subset=('song_id', 'title', 'artist_id'))

    #songs_table.createOrReplaceTempView("song_table")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs/songs_table.parquet')
    
    # extract columns to create artists table
    artists_table = song_df \
                  .select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
                  .dropDuplicates() \
                  .dropna(subset=('artist_id', 'artist_name'))
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'songs/artists_table.parquet')


def process_log_data(spark, song_df, log_df, output_data):
    ## get filepath to log data file
    #log_data = input_data + "log_data/*.json"
#
    ## read log data file
    #log_df = spark.read.json(log_data)

    ## read in song data to use for songplays table
    #song_data = input_data + "song_data/*/*/*/*.json"
    #song_log_df = spark.read.json(song_data)
    
    # filter by actions for song plays
    log_df = log_df.where("page='NextSong'")

    # extract columns for users table    
    users_table = log_df \
                  .select('userId', 'firstName', 'lastName', 'gender', 'level') \
                  .dropDuplicates() \
                  .dropna(subset=('userId', 'firstName'))
            
    # write artists table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'logs/users_table.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str( int(x)//1000 ))
    log_df = log_df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x))))

    log_df = log_df.withColumn('datetime', get_datetime('timestamp'))
    
    # extract columns to create time table
    time_table = log_df \
                 .select('datetime') \
                 .withColumn('hour', hour('datetime')) \
                 .withColumn('day', dayofmonth('datetime')) \
                 .withColumn('week', weekofyear('datetime')) \
                 .withColumn('month', month('datetime')) \
                 .withColumn('year', year('datetime')) \
                 .withColumn('weekday', dayofweek('datetime')) \
                 .dropDuplicates() \
                 .dropna(subset=('datetime'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'logs/time_table.parquet')

    log_df = log_df.alias('log_df')
    song_df = song_df.alias('song_df')
    
    log_song_df = log_df.join(song_df, col('log_df.artist') == col('song_df.artist_name'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_song_df.select( \
                      col('log_df.sessionId').alias('session_id'), \
                      col('log_df.datetime').alias('start_time'), \
                      col('log_df.userId').alias('user_id'), \
                      col('log_df.level').alias('level'), \
                      col('song_df.song_id').alias('song_id'), \
                      col('song_df.artist_id').alias('artist_id'), \
                      col('log_df.location').alias('location'),  \
                      col('log_df.userAgent').alias('user_agent'), \
                      year('log_df.datetime').alias('year'), \
                      month('log_df.datetime').alias('month')) \
                      .dropDuplicates() \
                      .dropna(subset=('session_id', 'song_id', 'artist_id', 'user_id'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'logs/songplays_table.parquet')

def loadData(spark, input_data, output_data):
    # get filepath to song data file 
    song_data = input_data + "song_data/A/A/A/*.json"
        
    # read song data file
    song_df = spark.read.json(song_data)

    # get filepath to log data file
    log_data = input_data + "log_data/*.json"
    # read log data file
    log_df = spark.read.json(log_data)

    return (song_df, log_df)

def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = "C:/GoogleDrive/Udacity/Data Engineering/project_4/data/"
    #output_data = "s3a://sabathh-us-west-2/"
    output_data = "C:/GoogleDrive/Udacity/Data Engineering/project_4/data/"
    
    song_df, log_df = loadData(spark, input_data, output_data)

    process_song_data(spark, song_df, output_data)    
    process_log_data(spark, song_df, log_df, output_data)


if __name__ == "__main__":
    main()
