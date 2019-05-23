""" 
    This file contains the functions to create  and drop the staging_events, staging_songs, songplays, users, songs, artists and time tables. Additionally, the create and drop queries are added to a list to be iteratively executed.
"""
import configparser


# CONFIG 
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ENDPOINT=config.get("CLUSTER","HOST")
DWH_DB= config.get("CLUSTER","DB_NAME")
DWH_DB_USER= config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD= config.get("CLUSTER","DB_PASSWORD")
DWH_PORT = config.get("CLUSTER","DB_PORT")
LOG_DATA_PATH = config.get("S3","LOG_DATA")
SONG_DATA_PATH = config.get("S3","SONG_DATA")
LOG_DATA_JSONPATH = config.get("S3","LOG_JSONPATH")
DWH_ARN = config.get("IAM_ROLE","ARN")

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
CREATE TABLE IF NOT EXISTS staging_events 
(artist VARCHAR(255),auth VARCHAR (20),firstname VARCHAR(255), gender CHAR(1), 
item_in_session INTEGER, lastname VARCHAR(255), length FLOAT, level VARCHAR(8), 
location VARCHAR(255), method VARCHAR(10), page VARCHAR(20), registration BIGINT, 
sessionid INTEGER, song VARCHAR (1024), status INTEGER,ts BIGINT, useragent TEXT, userid INTEGER)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(num_songs INTEGER, artist_id VARCHAR(255), artist_latitude FLOAT, artist_longitude FLOAT, 
artist_location VARCHAR (255), artist_name VARCHAR(255), song_id VARCHAR(255), title VARCHAR(255), 
duration FLOAT, year INTEGER)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(songplay_id INTEGER  IDENTITY(0,1) PRIMARY KEY, start_time TIMESTAMP NOT NULL sortkey,user_id VARCHAR(255) NOT NULL,
level VARCHAR(8),song_id VARCHAR(255) distkey, artist_id VARCHAR(255),session_id INTEGER, 
location VARCHAR(255),user_agent TEXT);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id VARCHAR(255) PRIMARY KEY sortkey, first_name VARCHAR(255), last_name VARCHAR(255),gender CHAR(1),level VARCHAR(8)) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR(255) PRIMARY KEY distkey , title VARCHAR(255), artist_id VARCHAR(255), year INTEGER, duration FLOAT) ;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(artist_id VARCHAR(255) PRIMARY KEY distkey, name VARCHAR(255),location VARCHAR(255),lattitude FLOAT, longitude FLOAT) ;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY sortkey distkey, hour INTEGER, day INTEGER, week INTEGER, month TEXT, year INTEGER, weekday TEXT) ;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    credentials 'aws_iam_role={}'
    json {} compupdate off region 'us-west-2';
""").format(LOG_DATA_PATH,DWH_ARN,LOG_DATA_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs
    from {}
    credentials 'aws_iam_role={}'
    json 'auto' compupdate off region 'us-west-2';
""").format(SONG_DATA_PATH,DWH_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT timestamp with time zone 'epoch' + se.ts/1000 * interval '1 second',se.userid,se.level,ss.song_id,ss.artist_id,se.sessionid,ss.artist_location,se.useragent 
    FROM staging_events se,staging_songs ss WHERE se.page='NextSong';
    
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT se.userid,se.firstname,se.lastname,se.gender,se.level
    FROM staging_events se WHERE se.userid IS NOT NULL AND se.page='NextSong';

""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT ss.song_id,ss.title,ss.artist_id,ss.year,ss.duration
    FROM staging_songs ss;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, lattitude, longitude)
    SELECT  DISTINCT ss.artist_id,ss.artist_name,ss.artist_location,ss.artist_latitude,ss.artist_longitude
    FROM staging_songs ss;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  sp.start_time, 
    EXTRACT (hour from sp.start_time),
    EXTRACT (day from sp.start_time) ,
    EXTRACT (week from sp.start_time),
    TO_CHAR (sp.start_time, 'MONTH'),
    EXTRACT (year from sp.start_time),
    TO_CHAR (sp.start_time, 'DAY')
    FROM songplays sp;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
