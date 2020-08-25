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
CREATE TABLE IF NOT EXISTS staging_events
(
  artist           varchar,
  auth             varchar,
  firstName        varchar,
  gender           varchar,
  itemInSession    integer,
  lastName         varchar,
  length           float,
  level            varchar,
  location         varchar,
  method           varchar,     
  page             varchar,
  registration     float,   
  sessionId        integer,
  song             varchar,
  status           integer,
  ts               timestamp,
  userAgent        varchar,
  userId           integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
  num_songs           integer,
  artist_id           varchar,
  artist_latitude     float,
  artist_longitude    float,
  artist_location     varchar,
  artist_name         varchar,
  song_id             varchar,
  title               varchar,
  duration            float,
  year                integer
);

""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
  songplay_id    integer      IDENTITY(0,1) primary key,
  start_time     timestamp    not null distkey sortkey,  
  user_id        integer      not null,
  level          varchar,
  song_id        varchar      not null,
  artist_id      varchar      not null,
  session_id     integer      not null,
  location       varchar,
  user_agent     varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
  user_id       integer      not null primary key sortkey,
  first_name    varchar      not null,
  last_name     varchar      not null,
  gender        varchar(1),
  level         varchar
)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
  song_id      varchar    not null primary key,
  title        varchar    not null sortkey,
  artist_id    varchar    not null distkey,
  year         integer,
  duration     float
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
  artist_id    varchar    not null primary key sortkey,
  name         varchar    not null,
  location     varchar,
  latitude     float,
  longitude    float
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
  start_time    timestamp    not null primary key distkey sortkey,
  hour          integer      not null,
  day           integer      not null,
  week          integer      not null,
  month         integer      not null,
  year          integer      not null,
  weekday       integer      not null
);
""")

# STAGING TABLES
IAM_ROLE = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    FORMAT AS JSON {}
    TIMEFORMAT 'epochmillisecs'
    BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    FORMAT AS JSON 'auto'
    BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    se.ts AS start_time,
    se.userId as user_id,
    se.level as level,
    ss.song_id as song_id,
    ss.artist_id as artist_id,
    se.sessionId as session_id,
    se.location as location,
    se.userAgent as user_agent
FROM staging_events se
LEFT JOIN staging_songs ss
ON (se.song = ss.title AND se.artist = ss.artist_name)
WHERE se.userId IS NOT NULL AND ss.song_id IS NOT NULL AND se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;        
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts,
    extract(hour from ts) as hour,
    extract(day from ts),
    extract(week from ts),
    extract(month from ts),
    extract(year from ts),
    extract(weekday from ts)
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]