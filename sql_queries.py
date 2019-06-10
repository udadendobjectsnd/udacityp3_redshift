import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                artist          VARCHAR(1000),
                                auth            VARCHAR(1000),
                                firstName       VARCHAR(1000),
                                gender          VARCHAR(1000),
                                itemInSession   INTEGER,
                                lastName        VARCHAR(1000),
                                length          FLOAT,
                                level           VARCHAR(1000),
                                location        VARCHAR(1000),
                                method          VARCHAR(1000),
                                page            VARCHAR(1000),
                                registration    VARCHAR(1000),
                                sessionId       INTEGER,
                                song            VARCHAR(1000),
                                status          VARCHAR(1000),
                                ts              BIGINT,
                                userAgent       VARCHAR(1000),
                                userId          INTEGER
                                ) 
""")
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                num_songs           INTEGER,
                                artist_id           VARCHAR(1000),
                                artist_latitude     FLOAT,
                                artist_longitude    FLOAT,
                                artist_location     VARCHAR(1000),
                                artist_name         VARCHAR(1000),
                                song_id             VARCHAR(1000),
                                title               VARCHAR(1000),
                                duration            FLOAT,
                                year                INTEGER
)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(
                            songplay_id         INT IDENTITY(1,1) PRIMARY KEY, 
                            start_time          BIGINT, 
                            user_id             INTEGER, 
                            level               VARCHAR(1000), 
                            song_id             VARCHAR(1000), 
                            artist_id           VARCHAR(1000), 
                            session_id          INTEGER, 
                            location            VARCHAR(1000), 
                            user_agent          VARCHAR(1000)
)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                                    user_id         INTEGER PRIMARY KEY, 
                                    first_name      VARCHAR(1000), 
                                    last_name       VARCHAR(1000), 
                                    gender          VARCHAR(1000), 
                                    level           VARCHAR(1000)
)
""")
song_table_create = ("""CREATE TABLE IF NOT EXISTS song(
                        song_id         VARCHAR(1000) PRIMARY KEY, 
                        title           VARCHAR(1000), 
                        artist_id       VARCHAR(1000), 
                        year            INTEGER, 
                        duration        FLOAT
)
""")
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist(
                            artist_id       VARCHAR(1000) PRIMARY KEY, 
                            name            VARCHAR(1000), 
                            location        VARCHAR(1000), 
                            lattitude       FLOAT, 
                            longitude       FLOAT
)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                            start_time  BIGINT PRIMARY KEY, 
                            hour        INTEGER, 
                            day         INTEGER,
                            week        INTEGER, 
                            month       INTEGER,
                            year        INTEGER,
                            weekday     BOOLEAN
)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time DATE PRIMARY KEY, \
                        hour      INT, \
                        day       INT, \
                        week      INT, \
                        month     INT, \
                        year      INT, \
                        weekday   INT);
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM 's3://udacity-dend/log-data'
credentials 'aws_iam_role={}'
region '{}' 
json 's3://udacity-dend/log_json_path.json'
""")

staging_songs_copy = ("""COPY staging_songs FROM 's3://udacity-dend/song-data'
credentials 'aws_iam_role={}'
region '{}' 
json 'auto' compupdate off statupdate off
""")

# FINAL TABLES

songplay_table_insert = ("""insert into song 
select 
song_id,
title,
artist_id,
year,
duration
from staging_songs
""")

user_table_insert = ("""INSERT INTO USERS 
select distinct userid,
firstname, 
lastname , 
gender, 
last_value(level) over (partition by userid order by sessionid asc
rows between unbounded preceding and unbounded following) 
from staging_events 
where userid is not null
order by sessionid, ts
""")

song_table_insert = ("""insert into songplay(start_time,
user_id   ,
level     ,
song_id   ,
artist_id ,
session_id,
location  ,
user_agent)
select 
se.ts as start_time,
se.userid as user_id    ,
se.level       ,
ss.song_id      ,
ss.artist_id     ,
se.sessionid as session_id     ,
se.location        ,
se.useragent as user_agent       
from staging_events se
join staging_songs ss
on upper(se.song) = upper(ss.title) and  upper(artist) = upper(ss.artist_name)
where se.userid is not null
and se.page = 'NextSong'
""")

artist_table_insert = ("""insert into artist
select distinct artist_id, 
artist_name, 
artist_location,
artist_latitude,
artist_longitude
from staging_songs
""")

time_table_insert = ("""insert into time
select  
ts as start_time  , 
EXTRACT ( hour FROM time_stamp ) as hour,
EXTRACT ( d FROM time_stamp ) as day   ,
EXTRACT ( w FROM time_stamp ) as week  , 
EXTRACT ( m  FROM time_stamp )as month ,
EXTRACT ( y FROM time_stamp )as year  ,
case when EXTRACT ( dw FROM time_stamp ) in (6,0) then True else false end as weekday
from
(select distinct ts, timestamp 'epoch' + ts/1000 * interval '1 second' time_stamp 
from staging_events) as se
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
