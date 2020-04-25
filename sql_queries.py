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
        artist_id TEXT,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location TEXT,
        artist_name TEXT,
        song_id TEXT,
        title TEXT,
        duration NUMERIC,
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
    user_id TEXT PRIMARY KEY SORTKEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    gender TEXT,
    level TEXT NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id TEXT PRIMARY KEY SORTKEY,
    title TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    year INTEGER,
    duration NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id TEXT PRIMARY KEY SORTKEY,
    name TEXT NOT NULL,
    location TEXT,
    latitude NUMERIC,
    longitude NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE time(
    start_time TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
);
""")

# STAGING TABLES
# Error while inserting timestamps
# See: https://stackoverflow.com/questions/28287434/how-to-insert-timestamp-column-into-redshift
# But we have epoch millisecs, so: 
# https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 's3://udacity-dend/log_json_path.json'
TIMEFORMAT AS 'epochmillisecs'
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES: DON'T INSERT null values

# songs: artist_id, song_id
# log: everything else
songplay_table_insert = (""" 
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT DISTINCT (ts) AS start_time, userId as user_id, level, ss.song_id, ss.artist_id,
sessionId as session_id, location, userAgent as user_agent
FROM staging_songs ss JOIN staging_events se 
ON (se.song = ss.title AND se.artist = ss.artist_name)
WHERE se.page='NextSong';
""")

user_table_insert = ("""
INSERT INTO USERS (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(userId) AS user_id, firstName AS first_name, lastName AS last_name, gender, level
FROM staging_events WHERE page='NextSong' AND user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO SONGS (song_id, title, artist_id, year, duration)
SELECT  DISTINCT(song_id), title, artist_id, year, duration
FROM staging_songs WHERE song_id IS NOT NULL;
""")

artist_table_insert = (""" 
INSERT INTO ARTISTS (artist_id, name, location, latitude, longitude)
SELECT DISTINCT(artist_id), artist_name AS name, artist_location AS location,
artist_latitude AS latitude, artist_longitude AS longitude
FROM staging_songs WHERE artist_id IS NOT NULL;
""")

# (SELECT DISTINCT (TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time) FROM 
#    ) AS TEMP: not necessary now: already ts in staging table.
time_table_insert = ("""
INSERT INTO TIME (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT(start_time), EXTRACT(hour FROM start_time) AS hour,
EXTRACT(day from start_time) AS day, EXTRACT(WEEK from start_time) AS week,
EXTRACT(month from start_time) AS month, EXTRACT(year from start_time) AS year,
EXTRACT(dow from start_time) AS dow FROM
  (SELECT DISTINCT(ts) AS start_time FROM staging_events se JOIN staging_songs ss
  ON (se.song = ss.title AND se.artist = ss.artist_name) 
  WHERE se.page='NextSong')  ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]