import os
import glob
import psycopg2
import pandas as pd
from datetime import datetime

from sql_queries import *


def process_song_file(cur, filepath):
    '''
    This function is dedicated to reading in JSONs from the data/song_data directory.
    Once a JSON is loaded, this function also transforms it, and formulates PostgreSQL
    queries for inserting the data into the right tables.
    '''
    df = pd.DataFrame([pd.read_json(filepath, typ='split')])

    # insert song record
    song_data = df.loc[0, ['song_id', 'title', 'artist_id', 'year', 'duration']]
    
    song_data = (
        song_data.song_id,
        song_data.title,
        song_data.artist_id,
        int(song_data.year),
        float(song_data.duration),
    )
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        import ipdb; ipdb.set_trace()
    
    # insert artist record
    artist_data = df.loc[0, ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = (
        artist_data.artist_id,
        artist_data.artist_name,
        artist_data.artist_location,
        artist_data.artist_latitude,
        artist_data.artist_longitude,
    ) 
    try:
        cur.execute(artist_table_insert, artist_data)
    except:
        import ipdb; ipdb.set_trace()
    


def process_log_file(cur, filepath):
    '''
    This function is dedicated to reading in JSONs from the data/log_data directory.
    Once a JSON is loaded, this function also transforms it, and formulates PostgreSQL
    queries for inserting the data into the right tables.
    '''
    df =  pd.DataFrame(pd.read_json(filepath, typ='split', lines=True).tolist())

    # filter by NextSong action
    df = df[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = (df['ts'] // 1000).apply(datetime.fromtimestamp)
    
    # insert time data records
    time_df = pd.DataFrame({
        'timestamp': t.apply(lambda one_t: int(one_t.timestamp())),
        'hour': t.dt.hour,
        'day': t.dt.day,
        'week': t.dt.weekofyear,
        'month': t.dt.month,
        'year': t.dt.year,
        'weekday': t.dt.weekday,
    })

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.length, row.length, row.artist))
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        if song_id is None or artist_id is None:
            # These are foreign keys, cannot be NULL
            return
        
        # insert songplay record
        songplay_data = (
            row.ts // 1000,  # start_time
            row.userId,  # user_id
            row.level,  # level
            song_id,  # song_id
            artist_id,  # artist_id
            row.sessionId,  # session_id
            row.location,  # location
            row.userAgent  # user_agent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    This is the central runner of the ETL.
    Depending on which func was passed, either log or song
    JSON files are loaded and transformed, and the changes to the
    tables in the database are being commited.
    '''
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    Run the whole ETL pipeline on the data/log_data and the data/song_data JSONs.
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()