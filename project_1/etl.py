import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Reads the JSON file in the provided filepath and adds data 
    from each row into song and artist tables 

    Args:
        cur : PostgreSQL cursor
        filepath (str): Path to file
    """    

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Reads the JSON file in the provided filepath and adds data 
    from each row into users, time and songplays tables 

    Args:
        cur : PostgreSQL cursor
        filepath (str): Path to file
    """    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"]=="NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms') 
    
    # insert time data records
    time_data = [[line.timestamp(), line.hour, line.day, line.week, line.month, line.year, line.weekday_name] for line in t]
    column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)


    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, pd.to_datetime(row.ts, unit='ms').timestamp(), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Retrieves all files available in the provided filepath and
    processes the using the provided func

    Args:
        cur : PostgreSQL cursor
        conn : Connection object with an estabilished session to a database
        filepath (str): Path to a file or folder
        func : Function to be used to process the file
    """    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Main ETL function

    Connects to the sparkifydb database and provides the folders 
    containing song data and log data to process_data() to store 
    the data into the database
    """    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()