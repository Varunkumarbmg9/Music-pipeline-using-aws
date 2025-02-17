import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    client_credentials_manager = SpotifyClientCredentials(client_id='7b2d63d5d69a4e8fa7a864be54040553', client_secret='fa86c94612f04522b43747c21cc5314d')
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    playlist_link='https://open.spotify.com/playlist/6UeSakyzhiEt4NB3UAd6NQ'
    playlist_uri=playlist_link.split('/')[-1]

    data = sp.playlist_tracks(playlist_uri)
    
    client = boto3.client('s3')

    filename = 'spotify_raw_' + str(datetime.now()) + '.json'

    client.put_object(
        Bucket='spotify-etl-project-atkuri',
        Key='raw_data/processed/' + filename,
        Body=json.dumps(data)
    )


