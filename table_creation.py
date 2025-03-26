CREATE TABLE IF NOT EXISTS artists (
    artistid VARCHAR(256) NOT NULL PRIMARY KEY,
    name VARCHAR(256),
    location VARCHAR(256),
    latitude NUMERIC(18,0),
    longitude NUMERIC(18,0)
);

CREATE TABLE IF NOT EXISTS songs (
    songid VARCHAR(256) NOT NULL PRIMARY KEY,
    title VARCHAR(256),
    artistid VARCHAR(256) NOT NULL,
    year INT4,
    duration NUMERIC(18,0),
    CONSTRAINT fk_artist FOREIGN KEY (artistid) REFERENCES artists(artistid)
);

CREATE TABLE IF NOT EXISTS users (
    userid INT4 NOT NULL PRIMARY KEY,
    first_name VARCHAR(256),
    last_name VARCHAR(256),
    gender VARCHAR(256),
    level VARCHAR(256)
);

CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    dayofweek INT
);

CREATE TABLE IF NOT EXISTS songplays (
    playid VARCHAR(32) NOT NULL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    userid INT4 NOT NULL,
    level VARCHAR(256),
    songid VARCHAR(256),
    artistid VARCHAR(256),
    sessionid INT4,
    location VARCHAR(256),
    user_agent VARCHAR(256),
    CONSTRAINT fk_time FOREIGN KEY (start_time) REFERENCES time(start_time),
    CONSTRAINT fk_user FOREIGN KEY (userid) REFERENCES users(userid),
    CONSTRAINT fk_song FOREIGN KEY (songid) REFERENCES songs(songid),
    CONSTRAINT fk_artist FOREIGN KEY (artistid) REFERENCES artists(artistid)
);
