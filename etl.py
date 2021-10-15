import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an argument.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True) 


    # insert song record
    song_data = [df.values[0][7],df.values[0][8],df.values[0][0],df.values[0][9],df.values[0][5]]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [df.values[0][0],df.values[0][4],df.values[0][2],df.values[0][1],df.values[0][3]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This procedure processes a log file whose filepath has been provided as an argument.
    It filters the data by NextSong action
    It then converts the timestamp column from milliseconds to datetime format
    Next, it proceeds to add the datetime in the following formats - 
    (hour,day,week,month, year and weekday) - as extra columns on the table. 
    It extracts the user information in order to store it into the user table.
    It then gets the song_id and artist_id from a join of the song and artist table;
    Then joins the data above with the songplay events data;
    And inserts the combined data rows into the songplay table.
    
    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the log file
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    #t = 
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    df['dates'] = df['ts'].dt.date
    df['hour'] = df['ts'].dt.hour
    df['day'] = df['ts'].dt.day
    df['week'] = df['ts'].dt.weekofyear
    df['month'] = df['ts'].dt.month
    df['year'] = df['ts'].dt.year
    df['weekday'] = df['ts'].dt.weekday
    df['weekday'].head()
    
    df = df.reset_index(drop=True)
    
    #time_data = 
    #column_labels = 
    time_df = df.filter(['ts','hour','day','week','month','year','weekday'], axis=1)
    
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df =  df.filter(['userId','firstName','lastName','gender','level'], axis=1)

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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This procedure gets all files in the directory
    
    INPUTS: 
    * cur the cursor variable
    * conn the connection to the database
    * filepath the file path to the song file
    * func the function to execute; which selects either
      the log or song data processing function
    
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
    This procedure connects to the database, creates the cursor, 
    processes the song and log data files, then closes the connection
            
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()