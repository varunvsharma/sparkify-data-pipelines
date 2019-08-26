class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (artist varchar,
                                                                                 auth varchar,
                                                                                 firstname varchar,
                                                                                 gender varchar,
                                                                                 iteminsession int,
                                                                                 lastname varchar,
                                                                                 length float,
                                                                                 level varchar,
                                                                                 location varchar,
                                                                                 method varchar,
                                                                                 page varchar,
                                                                                 registration float,
                                                                                 sessionid int,
                                                                                 song varchar,
                                                                                 status int,
                                                                                 ts timestamp,
                                                                                 useragent varchar,
                                                                                 userid int)
    """)

    staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (num_songs int,
                                                                               artist_id varchar(18),
                                                                               artist_latitude float,
                                                                               artist_longitude float,
                                                                               artist_location varchar,
                                                                               artist_name varchar,
                                                                               song_id varchar(18),
                                                                               title varchar,
                                                                               duration float,
                                                                               year int)
    """)

    
    songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (songplay_id varchar(32),
                                                                     start_time timestamp NOT NULL,
                                                                     userid int NOT NULL,
                                                                     level varchar,
                                                                     song_id varchar(18) NOT NULL DISTKEY,
                                                                     artist_id varchar(18) NOT NULL,
                                                                     sessionid int,
                                                                     location varchar,
                                                                     useragent varchar,
                                                                     PRIMARY KEY (songplay_id))
    """)

    user_table_create = ("""CREATE TABLE IF NOT EXISTS users (userid int,
                                                              firstname varchar,
                                                              lastname varchar,
                                                              gender varchar,
                                                              level varchar,
                                                              PRIMARY KEY (user_id))
                                                              DISTSTYLE AUTO
    """)

    song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar(18) DISTKEY,
                                                              title varchar,
                                                              artist_id varchar(18) NOT NULL,
                                                              year int,
                                                              duration float,
                                                              PRIMARY KEY (song_id))
                                                              DISTSTYLE KEY
    """)

    artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar(18),
                                                                  artist_name varchar,
                                                                  artist_location varchar,
                                                                  artist_latitude float,
                                                                  artist_longitude float,
                                                                  PRIMARY KEY (artist_id))
                                                                  DISTSTYLE AUTO
    """)

    time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp,
                                                             hour int,
                                                             day int,
                                                             week int,
                                                             month int,
                                                             year int,
                                                             weekday int,
                                                             PRIMARY KEY (start_time))
                                                             DISTSTYLE AUTO
    """)




