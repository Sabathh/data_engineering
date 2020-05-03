import configparser


# CONFIG
config = configparser.RawConfigParser(allow_no_value=True)
config.read('project_3/dwh.cfg')

LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']
ARN = config['IAM_ROLE']['ARN']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS 
                                    staging_events_table (
                                        event_id    BIGINT IDENTITY(0,1)    NOT NULL,
                                        artist              varchar,
                                        auth                varchar,
                                        first_name          varchar,
                                        gender              varchar,
                                        items_in_session    int,
                                        last_name           varchar,
                                        length              float,
                                        level               varchar,
                                        location            varchar,
                                        method              varchar,
                                        page                varchar,
                                        registration        varchar,
                                        session_id          int,
                                        song                varchar,
                                        status              int,
                                        ts                  bigint,
                                        user_agent          varchar,
                                        user_id             int
                                    )
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS 
                                    staging_songs_table (
                                        num_songs           int,
                                        artist_id           varchar,
                                        artist_latitude     float, 
                                        artist_longitude    float, 
                                        artist_location     varchar, 
                                        artist_name         varchar, 
                                        song_id             varchar, 
                                        title               varchar, 
                                        duration            float, 
                                        year                int
                                    )
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS
                                songplays (
                                    songplay_id     int IDENTITY(0,1) PRIMARY KEY,
                                    start_time      TIMESTAMP NOT NULL REFERENCES time(start_time), 
                                    user_id         int NOT NULL REFERENCES users(user_id), 
                                    level           varchar, 
                                    song_id         varchar REFERENCES songs(song_id), 
                                    artist_id       varchar REFERENCES artists(artist_id), 
                                    session_id      int, 
                                    location        varchar, 
                                    user_agent      varchar
                                )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS 
                            users (
                                user_id     int PRIMARY KEY, 
                                first_name  varchar NOT NULL, 
                                last_name   varchar, 
                                gender      varchar, 
                                level       varchar
                            )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS 
                            songs (
                                song_id     varchar PRIMARY KEY, 
                                title       varchar NOT NULL, 
                                artist_id   varchar NOT NULL, 
                                year        int, 
                                duration    float NOT NULL
                            )
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS 
                            artists (
                                artist_id   varchar PRIMARY KEY, 
                                name        varchar NOT NULL, 
                                location    varchar, 
                                latitude    float, 
                                longitude   float
                            )
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS 
                            time (
                                start_time  TIMESTAMP PRIMARY KEY, 
                                hour        int, 
                                day         int, 
                                week        int, 
                                month       int, 
                                year        int, 
                                weekday     varchar
                            )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events_table FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs_table FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
                        start_time,
                        user_id,
                        level,
                        song_id,
                        artist_id,
                        session_id,
                        location,
                        user_agent
    )
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'   AS start_time,
            se.user_id                     AS user_id,
            se.level                       AS level,
            ss.song_id                     AS song_id,
            ss.artist_id                   AS artist_id,
            se.session_id                  AS session_id,
            se.location                    AS location,
            se.user_agent                  AS user_agent
        FROM staging_events_table as se
            JOIN staging_songs_table as ss
                ON (se.artist = ss.artist_name)
            WHERE se.page='NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (
                        user_id,   
                        first_name,
                        last_name, 
                        gender,    
                        level     
    )
    SELECT  se.user_id      AS user_id,
            se.first_name   AS first_name,
            se.last_name    AS last_name, 
            se.gender       AS gender, 
            se.level        AS level    
        FROM    staging_events_table AS se
        WHERE   se.page='NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (
            song_id,                           
            title,                   
            artist_id,               
            year,                    
            duration                
    )
    SELECT  ss.song_id      AS song_id,  
            ss.title        AS title,    
            ss.artist_id    AS artist_id,
            ss.year         AS year,     
            ss.duration     AS duration  
        FROM staging_songs_table AS ss
""")

artist_table_insert = ("""
    INSERT INTO artists (
            artist_id,
            name,     
            location, 
            latitude, 
            longitude            
    )
    SELECT  ss.artist_id            AS artist_id,
            ss.artist_name          AS name,     
            ss.artist_location      AS location, 
            ss.artist_latitude      AS latitude, 
            ss.artist_longitude     AS longitude
        FROM staging_songs_table AS ss
""")

time_table_insert = ("""
    INSERT INTO time (
            start_time,
            hour,      
            day,       
            week,      
            month,     
            year,      
            weekday   
    )
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'           AS start_time,
            EXTRACT(hour FROM start_time)       AS hour,   
            EXTRACT(day FROM start_time)        AS day,    
            EXTRACT(week FROM start_time)       AS week,   
            EXTRACT(month FROM start_time)      AS month,  
            EXTRACT(year FROM start_time)       AS year,   
            EXTRACT(dow FROM start_time)        AS weekday
        FROM staging_events_table AS se
        WHERE se.page='NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
