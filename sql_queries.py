import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender TEXT,
    itemInSession INTEGER,
    lastName TEXT,
    length NUMERIC,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration NUMERIC,
    sessionId INTEGER,
    song TEXT,
    status INTEGER,
    ts TIMESTAMP,
    userAgent TEXT,
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
        num_songs INTEGER,
        artist_id TEXT
        artist_latitude NUMERIC
        artist_longitude NUMERIC
        artist_location TEXT
        artist_name TEXT
        song_id TEXT
        title TEXT
        duration NUMERIC
        year INTEGER
);
""")
# Distkey on time column, because time dim is large.
songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY DISTKEY,
    user_id INTEGER NOT NULL,
    level TEXT,
    song_id TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    session_id INTEGER,
    location TEXT,
    user_agent TEXT
);
""")

user_table_create = ("""
CREATE TABLE users(
    user_id TEXT SORTKEY PRIMARY KEY,
    first_name NOT NULL,
    last_name NOT NULL,
    gender NOT NULL,
    level NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id TEXT SORTKEY PRIMARY KEY,
    title TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    duration NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id TEXT SORTKEY PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT,
    latitude NUMERIC,
    longitude NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE time(
    start_time TIMESTAMP SORTKEY DISTKEY PRIMARY KEY,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {bucket}
CREDENTIALS 'aws-iam-role={arn}'
REGION 'us-west-2'
""").format()

staging_songs_copy = ("""
COPY staging_songs FROM {bucket}
CREDENTIALS 'aws-iam-role={arn}''
REGION 'us-west-2'
""").format()

# FINAL TABLES: DON'T INSERT null values

# songs: artist_id, song_id
# log: everything else
songplay_table_insert = (""" 
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT DISTINCT se.start_time, se.user_id, se.level, ss.song_id, ss.artist_id, se.session_id,
                
""")

user_table_insert = ("""
INSERT INTO USERS (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(userId) AS user_id, firstName AS first_name, lastName AS last_name, gender, level
FROM staging_events WHERE page='NextSong' AND user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO SONGS (song_id, title, artist_id, year, duration)
SELECT  DISTINCT(songId) AS song_id, title, artist_id, year, duration
FROM staging_songs WHERE song_id IS NOT NULL;
""")

artist_table_insert = (""" 
INSERT INTO ARTISTS (artist_id, name, location, latitude, longitude)
SELECT DISTINCT(artistId) AS artist_id, artist_name AS name, artist_location AS location,
artist_latitude AS latitude, artist_longitude AS longitude
FROM staging_songs WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
