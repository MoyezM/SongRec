import psycopg2
import pandas as pd
import numpy as np
import constants as c
from dataclasses import dataclass
from typing import List
from tqdm import tqdm

@dataclass
class song:
    id : str
    name : str
    artists : List[str]
    danceability : float
    energy : float
    key : float
    loudness : float
    mode : float
    speechiness : float
    acousticness : float
    instrumentalness : float
    liveness : float
    valence : float
    tempo : float
    duration_ms : int
    time_signature : int
        
@dataclass
class playlist:
    name : str
    id : str
    tracks_url : str
    owner_id : str
    genres : List[str]
    songs : List[song]

class DatasetGenerator:
    def __init__(self, batch_size):
        self.batch_size = batch_size
        self.sql_conn('PlaylistScraper') 
    
    def sql_conn(self, db_name):
        self.conn = psycopg2.connect(host="localhost", port=5432, database=db_name, user=c.user, password=c.password)
        self.cur = self.conn.cursor()
        
        return
    
#   note this will only work with one col at a time
    def get_all_songs(self, col):
        try:
            q = \
            f'''
            SELECT 
            {col}
            FROM songs
            '''  
            
            self.cur.execute(q)
            data = self.cur.fetchall()
            
            return data
        
        except Exception as e:
            self.sql_conn('PlaylistScraper') 
            print(e)
            
            
            
        
    def get_songs(self, playlist_id):
        try:
            q = \
            """
            SELECT 
            s.id, s.name, s.artists_id,
            f.danceability, f.energy, f.key, f.loudness, f.mode, f.speechiness, f.acousticness, f.instrumentalness, f.liveness, f.valence, f.tempo, f.duration_ms, f.time_signature
            FROM playlist_songs p 
            INNER JOIN songs as s 
            ON p.song_id = s.id
            INNER JOIN song_features as f
            ON f.id = p.song_id
            where p.playlist_id = %s
            """
            self.cur.execute(q, (playlist_id,))
            raw_songs = self.cur.fetchall()
            songs = [song(*s) for s in raw_songs]

            return songs

        except Exception as e:
            self.sql_conn('PlaylistScraper') 
            print(e)
            
    def get_genres(self, playlist_id):
        try:
            q = "select genre from playlist_genres where playlist_id = %s"
            self.cur.execute(q, (playlist_id,))
            raw_genres = self.cur.fetchall()
            return [x[0] for x in raw_genres]
        
        except Exception as e:
            self.sql_conn('PlaylistScraper') 
            print(e)
    
    def get_playlist(self, playlist_id):
        try:
            q = "select * from playlists where id = %s"

            self.cur.execute(q, (playlist_id,))
            p = list(self.cur.fetchall()[0])

            genres = self.et_genres(playlist_id)
            p.append(genres)
            p.append(self.get_songs(playlist_id))

            return playlist(*p)

        except Exception as e:
            self.sql_conn('PlaylistScraper') 
            print(e)
            
    def get_playlists(self):
        q = "select id from playlists"
        self.cur.execute(q)
        return self.cur.fetchall()

    
    def generator(self, ids, batch_size):
        i = 0

        batches = ( len(ids) // batch_size )  + 1
        while i < batches:
            batch_ids = ids[i*batch_size : (i + 1) * batch_size]
            playlists = []
            for _id in batch_ids:
                playlists.append(get_playlist(_id))


            i += 1

            yield playlists
            
            
    def create_generator(self):
        _ids = self.get_playlists()
        return generator(_ids, self.batch_size)
