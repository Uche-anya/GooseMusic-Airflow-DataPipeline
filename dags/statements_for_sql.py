# Optimized SQL Table Creation and Insert Statements

# Staging Events Table
CREATE_staging_events_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(256),
    auth VARCHAR(50),
    first_name VARCHAR(50),
    gender CHAR(1),
    item_in_session INT,
    last_name VARCHAR(50),
    length FLOAT,
    level VARCHAR(50),
    location VARCHAR(256),
    method VARCHAR(10),
    page VARCHAR(50),
    registration BIGINT,
    session_id INT,
    song VARCHAR(256),
    status INT,
    ts BIGINT,
    user_agent TEXT,
    user_id INT
);
"""

# Staging Songs Table
CREATE_staging_songs_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR(50),
    artist_name VARCHAR(256),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(256),
    song_id VARCHAR(50),
    title VARCHAR(256),
    duration FLOAT,
    year INT
);
"""

# Artists Table
CREATE_artists_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(256),
    location VARCHAR(256),
    latitude FLOAT,
    longitude FLOAT
);
"""

# Songplays Fact Table
CREATE_songplays_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS songplays (
    play_id VARCHAR(32) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR(50),
    song_id VARCHAR(50),
    artist_id VARCHAR(50),
    session_id INT,
    location VARCHAR(256),
    user_agent TEXT
);
"""

# Songs Table
CREATE_songs_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(256),
    artist_id VARCHAR(50),
    year INT,
    duration FLOAT
);
"""

# Users Table
CREATE_users_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender CHAR(1),
    level VARCHAR(50)
);
"""

# Time Table
CREATE_time_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    day_of_week INT
);
"""

# Insert Statements
songplay_table_insert = """
    SELECT DISTINCT
        MD5(events.start_time || events.session_id) AS play_id,
        events.start_time, 
        events.user_id, 
        events.level, 
        songs.song_id, 
        songs.artist_id, 
        events.session_id, 
        events.location, 
        events.user_agent
    FROM (
        SELECT TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS start_time, *
        FROM staging_events
        WHERE page = 'NextSong'
    ) events
    LEFT JOIN staging_songs songs
    ON events.song = songs.title
       AND events.artist = songs.artist_name
       AND events.length = songs.duration
    WHERE events.user_id IS NOT NULL;
"""

user_table_insert = """
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE page = 'NextSong';
"""

song_table_insert = """
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs;
"""

artist_table_insert = """
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs;
"""

time_table_insert = """
    SELECT start_time,
           EXTRACT(hour FROM start_time),
           EXTRACT(day FROM start_time),
           EXTRACT(week FROM start_time),
           EXTRACT(month FROM start_time),
           EXTRACT(year FROM start_time),
           EXTRACT(dow FROM start_time)
    FROM songplays;
"""
