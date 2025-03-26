class SqlQueries:
    """
    This class contains SQL queries to insert data into fact and dimension tables.
    Data is extracted from staging tables and formatted for easier analysis.
    """

    # 1. Insert Data into the Fact Table (songplays)
    songplay_table_insert = """
        SELECT
            md5(events.sessionid || events.start_time) AS songplay_id,  -- Unique ID
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM 
            (SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time, * 
             FROM staging_events
             WHERE page='NextSong') events  -- Filter only song play events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
           AND events.artist = songs.artist_name
           AND events.length = songs.duration;
    """

    # 2. Insert Data into the Users Dimension Table
    user_table_insert = """
        SELECT DISTINCT userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong';  -- Only include users who played a song
    """

    # 3. Insert Data into the Songs Dimension Table
    song_table_insert = """
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs;
    """

    # 4. Insert Data into the Artists Dimension Table
    artist_table_insert = """
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs;
    """

    # 5. Insert Data into the Time Dimension Table
    time_table_insert = """
        SELECT 
            start_time, 
            EXTRACT(hour FROM start_time), 
            EXTRACT(day FROM start_time), 
            EXTRACT(week FROM start_time), 
            EXTRACT(month FROM start_time), 
            EXTRACT(year FROM start_time), 
            EXTRACT(dow FROM start_time)  -- Day of the week
        FROM songplays;
    """
