import os
import glob
import datetime
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Opens and reads JSON song file <filepath> into Pandas DataFrame.

    - Extracts song attributes from the data frame and executes corresponding SQL Query
    to insert a record for this song into the songs table.

    - Extracts artist attributes from the data frame and executes corresponding SQL Query
    to insert a record for this artist into the artists table.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values
    cur.execute(song_table_insert, list(song_data[0]))

    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].iloc[0].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Opens, reads and processes JSON log file <filepath> into Pandas DataFrame.

    - Iterates over all rows in processed data frame. For each row extracts
    time attributes and executes corresponding SQL Query to insert a record
    into the time table.

    - Iterates over all rows in processed data frame. For each row extracts
    user attributes and executes corresponding SQL Query to insert a record
    into the users table.

    - Iterates over all rows in processed data frame. For each row extracts
    songplay attributes. In addition, queries the songs and artists tables to find
    the rest of required attributes.
    For the resulting set executes corresponding SQL Query to insert a record
    into the songplays table.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == 'NextSong']

    # convert timestamp column to datetime
    t = df['ts'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000.0))

    # insert time data records (for newer Python versions)
    # time_data = [*zip(t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday)]
    # insert time data records
    time_data = [*zip(t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels,zip(*time_data))))

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
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (t.loc[index], row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Gets and enumerates all JSON files in <filepath> subdirectory.

    - Iterates over each found file and processes it using db cursor <cur> and function <func>.

    - After each iteration makes commit to the database <conn>.
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
    - Establishes connection with the sparkify database and gets
    cursor to it.

    - Reads and processes all JSON files in subdir "data/song_data".

    - Reads and processes all JSON files in subdir "data/log_data".

    - Finally, closes the connection.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
