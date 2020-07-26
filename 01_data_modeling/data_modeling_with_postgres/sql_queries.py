# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP REFERENCES time (start_time),
    user_id INT REFERENCES users (user_id),
    level VARCHAR,
    song_id VARCHAR REFERENCES songs (song_id),
    artist_id VARCHAR REFERENCES artists (artist_id),
    session_id INT NOT NULL,
    location VARCHAR,
    user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR(1),
    level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT CHECK (year >= 0),
    duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT NOT NULL CHECK (hour >= 0 and hour <= 24),
    day INT NOT NULL CHECK (day >= 0 and day <= 366),
    week INT NOT NULL CHECK (week >= 0 and week <= 55),
    month INT NOT NULL CHECK (month >= 0 and month <= 12),
    year INT NOT NULL CHECK (year >= 0),
    weekday INT NOT NULL CHECK (weekday >= 0 and weekday <= 7)
);
""")


# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (
    songplay_id,
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id) DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO NOTHING;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING;
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
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT 
    songs.song_id as song_id,
    songs.artist_id as artist_id
FROM 
    songs 
JOIN artists 
    ON songs.artist_id = artists.artist_id
WHERE 
    songs.title = %s AND 
    artists.name = %s AND 
    songs.duration = %s
""")

# QUERY LISTS


create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create] # change order of table creation
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]