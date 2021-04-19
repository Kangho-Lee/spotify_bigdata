 # -*- coding: utf-8 -*-
import sys
import requests
import base64
import json
import logging
import pymysql
import csv
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
import pandas as pd
import jsonpath


## Variable Information
client_id = ""
client_secret = ""
host = ""
port = 3306
username = ""
database = ""
password = ""


## Configuration
# (A) Connecting RDS(MySQL)
try:
    conn = pymysql.connect(host=host, user=username, passwd=password, db=database, port=port, use_unicode=True, charset='utf8', cursorclass=pymysql.cursors.DictCursor)
    cursor = conn.cursor()
except Exception as e:
    logging.error(e)
    logging.exception(e)
    logging.error("Could not connect to RDS!")
    raise


# (B) Connecting Dynamodb
try:
    dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-2', endpoint_url='http://dynamodb.ap-northeast-2.amazonaws.com')
except:
    logging.error('could not connect to dynamodb')
    sys.exit(1)


# artist_list.csv에 사전에 셋팅한 가수들의 이름을 spotify에서 search후 유효한 데이터(가수)에 대하여 RDS에 insert
def insert_artist_to_rds():
    headers = get_headers(client_id, client_secret)

    ## Spotify Search API
    artists = []
    with open('artist_list.csv') as f:
        raw = csv.reader(f)
        for row in raw:
            artists.append(row[0])

    for artist_nm in artists:
        params = {
            "q": artist_nm,
            "type": "artist",
            "limit": "1"
        }

        r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)
        raw = json.loads(r.text)

        artist = {}
        try:
            artist_raw = raw['artists']['items'][0]
            if artist_raw['name'] == params['q']:
                artist.update(
                    {
                        'id': artist_raw['id'],
                        'name': artist_raw['name'],
                        'followers': artist_raw['followers']['total'],
                        'popularity': artist_raw['popularity'],
                        'url': artist_raw['external_urls']['spotify'],
                        'image_url': artist_raw['images'][0]['url']
                    }
                )
                insert_row(cursor, artist, 'artists')
        except:
            logging.error(f'No items from search API - {artist_nm}')
            continue
    conn.commit()


# artists 테이블에서 50개씩 가수를 가져와서 search(batch) 후 artist의 genre를 RDS에 넣음
def insert_artist_genres_to_rds():
    cursor.execute("SELECT id FROM artists")
    artists = []
    for data in cursor.fetchall():
        artists.append(data['id'])

    artist_batch = [artists[i: i+50] for i in range(0, len(artists), 50)]
    artist_genres = []
    for i in artist_batch:
        ids = ','.join(i)
        URL = "https://api.spotify.com/v1/artists/?ids={}".format(ids)
        r = requests.get(URL, headers=headers)
        raw = json.loads(r.text)
        for artist in raw['artists']:
            for genre in artist['genres']:
                artist_genres.append(
                    {
                        'artist_id': artist['id'],
                        'genre': genre
                    }
                )

    for data in artist_genres:
        insert_row(cursor, data, 'artist_genres')

    conn.commit()


# artists의 테이블에서 id를 가져와서 top-track들을 조회하여 ddb에 저장
def insert_top_tracks_to_ddb():
    headers = get_headers(client_id, client_secret)
    table = dynamodb.Table('top_tracks')

    cursor.execute('SELECT id FROM artists')

    for artist in cursor.fetchall():
        URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist['id'])
        params = {
            'country': 'US'
        }
        r = requests.get(URL, params=params, headers=headers)
        raw = json.loads(r.text)

        for track in raw['tracks']:
            data = {
                'artist_id': artist['id']
            }

            data.update(track)
            table.put_item(
                Item=data
            )


# artists의 테이블에서 id를 가져와서 top-track들을 조회하여 S3에 저장
def insert_top_tracks_audio_features_to_s3():
    headers = get_headers(client_id, client_secret)

    # (1) - Top Tracks to S3
    # RDS - 아티스트ID 를 가져오고
    cursor.execute("SELECT id FROM artists limit 10")

    # Parquet 변환을 위함 (Data 형식 - 값들이 복자하게 리스트화 되어있음)
    top_track_keys = {
        "id": "id",
        "name": "name",
        "popularity": "popularity",
        "external_url": "external_urls.spotify"
    }

    # Top Tracks 를 Spotify 통해 가져오고
    top_tracks = []
    for artist in cursor.fetchall():
        URL = 'https://api.spotify.com/v1/artists/{}/top-tracks'.format(artist['id'])
        params = {
            'country' : 'US'
        }
        r = requests.get(URL, params=params, headers=headers)
        raw = json.loads(r.text)

        for i in raw['tracks']:
            top_track = {}
            for k, v in top_track_keys.items():
                top_track.update({k: jsonpath.jsonpath(i, v)})
                top_track.update({'artist_id': artist['id']})
                top_tracks.append(top_track)

    top_tracks_pd = pd.DataFrame(top_tracks)
    top_tracks_pd.to_parquet('top-tracks.parquet', engine='pyarrow', compression='snappy')

    dt = datetime.utcnow().strftime("%Y-%m-%d")
    s3 = boto3.resource('s3')
    object = s3.Object('fc-spotify-artist', f'top-tracks/dt={dt}/top-tracks.parquet')
    data = open('top-tracks.parquet', 'rb')
    object.put(Body=data)

    # (2) - Audio Features to S3
    # track_ids
    track_ids = [i['id'][0] for i in top_tracks]
    tracks_batch = [track_ids[i: i+100] for i in range(0, len(track_ids), 100)]

    audio_features = []
    for i in tracks_batch:
        ids = ','.join(i)
        URL = "https://api.spotify.com/v1/audio-features/?ids={}".format(ids)

        r = requests.get(URL, headers=headers)
        raw = json.loads(r.text)
        audio_features.extend(raw['audio_features'])

    audio_features_pd = pd.DataFrame(audio_features)
    audio_features_pd.to_parquet('audio-features.parquet', engine='pyarrow', compression='snappy')

    s3 = boto3.resource('s3')
    object = s3.Object('fc-spotify-artist', 'audio-features/dt={}/audio-features.parquet'.format(dt))
    data = open('audio-features.parquet', 'rb')
    object.put(Body=data)



def main():
    # insert_artist_to_rds()
    # insert_artist_genres_to_rds()
    # insert_top_tracks_to_ddb()
    insert_top_tracks_audio_features_to_s3()




def get_headers(client_id, client_secret):
    endpoint = "https://accounts.spotify.com/api/token"
    encoded = base64.b64encode(f"{client_id}:{client_secret}".encode('utf-8')).decode('ascii')

    headers = {
        "Authorization": f"Basic {encoded}"
    }
    payload = {
        "grant_type": "client_credentials"
    }
    r = requests.post(endpoint, data=payload, headers=headers)
    access_token = json.loads(r.text)['access_token']

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    return headers


def insert_row(cursor, data, table):
    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=%s'.format(k) for k in data.keys()])
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)
    # INSERT INTO artists ( id, name, followers, popularity, url, image_url ) VALUES ( %s, %s, %s, %s, %s, %s ) ON DUPLICATE KEY UPDATE id=%s, name=%s, followers=%s, popularity=%s, url=%s, image_url=%s
    cursor.execute(sql, list(data.values())*2)


if __name__=='__main__':
    main()
