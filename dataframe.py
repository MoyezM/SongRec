import psyplaylist_dfpg2
import pandas as pd
import numpy as np
import constants as c
from tqdm import tqdm
import gc

def sql_conn(db_name):
    conn = psycopg2.connect(host="localhost", port=5432, database=db_name, user=c.user, password=c.password)
    cur = conn.cursor()

    return (conn, cur)

def song_df(save=False):
    cols = ['id', 'name', 'artists_id', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms', 'time_signature']
    conn, cur  = sql_conn('PlaylistScraper')  
    
    q = \
    """
    SELECT s.id, s.name, s.artists_id,
    f.danceability, f.energy, f.key, f.loudness, f.mode, f.speechiness, f.acousticness, f.instrumentalness, f.liveness, f.valence, f.tempo, f.duration_ms, f.time_signature
    FROM songs s
    INNER JOIN song_features f
    ON f.id = s.id
    """
    
    cur.execute(q)
    data = cur.fetchall()
    
    df = pd.DataFrame(data, columns=cols)    
    
    del data
    
    if save:
        df.to_pickle('dataframes/songs.pickle')
        return
    
    return df

def get_playlist(_id):
    conn, cur  = sql_conn('PlaylistScraper')  
    
    q = \
    """
    SELECT p.name, p.owner_id
    FROM playlists p
    WHERE p.id = %s
    """
    
    cur.execute(q, (_id, ))
    name, owner_id = cur.fetchall()[0]
    
    q = \
    """
    SELECT g.genre
    FROM playlist_genres g
    where g.playlist_id = %s
    """
    
    cur.execute(q, (_id, ))
    genres = [t[0] for t in cur.fetchall()]
    
    q = \
    """
    SELECT s.song_id
    FROM playlist_songs s
    WHERE s.playlist_id = %s
    """
    
    cur.execute(q, (_id, ))
    songs = [t[0] for t in cur.fetchall()]
    
    
    return _id, name, owner_id, genres, songs


def playlist_df(save=False):
    
    conn, cur  = sql_conn('PlaylistScraper')  
    
    q = \
    """
    SELECT p.id
    FROM playlists p
    """
    
    cur.execute(q)
    playlist_ids = [t[0] for t in cur.fetchall()]
    cols = ['id', 'name', 'owner_id','genres', 'songs']

    df = pd.DataFrame(columns=cols)
    
    for _id in tqdm(playlist_ids):
        playlist = get_playlist(_id)
        df = df.append(pd.Series(playlist, index=df.columns), ignore_index=True)
        del playlist
        
    if save:
        df.to_pickle('dataframes/playlists.pickle')
        return
    
    return df   
    

def load_df():
    song_df = pd.read_pickle('dataframes/songs.pickle')
    playlist_df = pd.read_pickle('dataframes/playlists.pickle')
    
    return song_df, playlist_df