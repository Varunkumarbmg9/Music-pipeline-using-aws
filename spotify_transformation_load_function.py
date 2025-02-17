import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd
def album(data):
    album_list=[]
    for row in data['items']:
        album_id=row['track']['album']['id']
        album_name=row['track']['album']['name']
        album_release_date=row['track']['album']['release_date']
        album_album_type=row['track']['album']['album_type']
        album_url=row['track']['album']['external_urls']['spotify']
        album_elements={'album_id':album_id,'name':album_name,'release_date':album_release_date,'type':album_album_type,'url':album_url}
        album_list.append(album_elements)
    return album_list

def artist(data):
    artist_list=[]
    for row in data['items']:
        for key, value in row.items():
            if key=='track':
                for artist in value['artists']:
                    artist_dict={'artist_id':artist['id'],'artist_name':artist['name'],'external_url':artist['href']}
                    artist_list.append(artist_dict)
    return artist_list


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'spotify-etl-project-atkuri'
    Key = "raw_data/processed/"

    spotify_data=[]
    spotify_keys=[]
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)


    for data in spotify_data:
        album_list=album(data)
        artist_list=artist(data)

        album_df=pd.DataFrame.from_dict(album_list)
        album_df=album_df.drop_duplicates(subset=['album_id'])

        artist_df=pd.DataFrame.from_dict(artist_list)
        artist_df=artist_df.drop_duplicates(subset=['artist_id'])

        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer=StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content=album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)

        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content=artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)


    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, "raw_data/to_processed/" + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()


