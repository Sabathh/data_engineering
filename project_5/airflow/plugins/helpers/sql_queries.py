class SqlQueries:
    songplay_table_insert = ("""
        SELECT DISTINCT
                TIMESTAMP 'epoch' + events.ts/1000 * interval '1 second'
                AS start_time,
                events.userid,
                events.level,
                songs.song_id,
                songs.artist_id,
                events.sessionid,
                events.location,
                events.useragent,
                md5(events.sessionid || start_time) AS playid
            FROM staging_events events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
            WHERE events.page = 'NextSong'
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
                AND events.userid IS NOT NULL
                AND events.level IS NOT NULL
                AND songs.song_id IS NOT NULL
                AND songs.artist_id IS NOT NULL
                AND events.sessionid IS NOT NULL
                AND events.location IS NOT NULL
                AND events.useragent IS NOT NULL
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong' AND userid is not NULL
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)