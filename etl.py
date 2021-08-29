import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Goal: 
            This function processes the JSON format song file. 
    
    Arguments:
            cursor: cursor linked to the database is required
            filepath: location of the file should be given
            
    Returns:
            This function will return two data records.
            1. song_data : song data will be inserted to song_table (columns: "song_id", "title", "artist_id", "year", "duration")
            2. artist_data : artist data will be inserted to artist_table (columns: "artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
            
    """
    
    # open song file
    df = pd.read_json(filepath, typ = 'series')

    # insert song record
    song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]].values)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values)
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Goal: 
            This function processes the JSON format log file. 
    
    Arguments:
            cursor: cursor linked to the database is required
            filepath: location of the file should be given
            
    Returns:
            This function will return three data records based on data filtered by NextSong action.
            1. time_data : time data will be inserted to time_table (columns: 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
            2. user_data : user data will be inserted to user_table (columns: "userId", "firstName", "lastName", "gender", "level")
            3. songplay : this dataset will get song_id and artist_id from song and artist tables and insert data to songplay_table (columns: time (in ms), userId, level, songid, artistid, sessionId, location, userAgent)
            
    """    

    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit = 'ms')
    
    # insert time data records
    time_data = pd.concat([t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday], axis = 1)
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(data = time_data.values, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]].drop_duplicates().dropna()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit = 'ms'), 
                         row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Goal: 
            This function processes the files. 
    
    Arguments:
            cursor: cursor linked to the database is required
            connection: connection to the database is required
            filepath: location of the file should be given
            function: input function with desired output
            
    Returns:
            Files will be processed.
            The process will be printed.
            
    """       
    
    # get all files matching extension from directory
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
    """

    Goal: 
            - Create connection
            - Set up a cursor
            - Process song_data and log_data
            - Close connection
    
    Arguments:
            None
            
    Returns:
            Achieve goal  

    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath = 'data/song_data', func = process_song_file)
    process_data(cur, conn, filepath = 'data/log_data', func = process_log_file)

    conn.close()


if __name__ == "__main__":
    main()