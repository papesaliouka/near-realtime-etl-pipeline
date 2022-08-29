import pandas as ps
from pymongo import MongoClient
import json
import sys
import redis
import functools


client = MongoClient('localhost',27017)
db = client['sentinel-dev']

r = redis.Redis(host='localhost', port=6379)

def getPathAndRedisKey():
    try:
        path = functools.reduce( lambda x,y: x+' '+y ,sys.argv[1:])
        poped = path.split('/').pop()
        redis_key = path.replace(f"/{poped}",'')
        print('filepath',path)
        print('redis-key',redis_key)
        return [path,redis_key]
    except:
        print('file not found or invalid key')
        return ['','']

def getComplement(redis_key):
    try:
        complement = json.loads(r.get(redis_key))
        print('redis data found',complement)
        return complement
    except:
        print('error unable to find redis data')
        raise 'redis error'

def add_complement(complement, wanted_df):
    try:
        for _,(key,value) in enumerate(complement.items()):
            wanted_df[key]=value
        print('added redis data to dataset')
        return wanted_df
    except:
        print('redis data not found')
        raise 'redis error'

def insert_to_releves(wanted_df):
    try:
        records = wanted_df.to_dict('records')
        db['test-pyspark23'].insert_many(records)
        print('data inserted successfullly to releves')
    except:
        print('insertion error')
        raise 'insertion error'

def insert_to_stats(arr):
    try:
        db['calcul_stats_heures'].insert_many(arr)
        print('inserted to calcul_stats_heures')
    except:
        raise 'insertion to stats error'



def format_digit(num):
    if num <=9:
        return '09'
    else:
        return str(num)

def stringyfy(df):
    df['jour'] = df['jour'].apply(format_digit)
    df['mois'] = df['mois'].apply(format_digit)
    df['heure'] = df['heure'].apply(format_digit)
    df['minute'] = df['minute'].apply(format_digit)
    df['seconde'] = df['seconde'].apply(format_digit)
    df['annee'] = df['annee'].apply(str)
    df['mois'] = df['mois'].apply(str)
    df['jour'] = df['jour'].apply(str)
    df['heure'] = df['heure'].apply(str)
    df['minute'] = df['minute'].apply(str)
    df['seconde'] = df['seconde'].apply(str)
    df['datetime'] = df['annee'] + '/' + df['mois'] + '/' + df['jour']+ ' ' + df['heure'] + ":" + df['minute'] +":" + df['seconde']
    df['datetime'] = ps.to_datetime(df['datetime'])
    df.index=df.datetime
    df.sort_index(inplace=True,ascending=False)
    return df

def make_datetime(wanted_df):
    if 'date' in wanted_df.columns:
        wanted_df['datetime'] = ps.to_datetime(wanted_df['date'] + ' ' + wanted_df['heure'])
        wanted_df.index = wanted_df.datetime
        wanted_df.sort_index(inplace=True,ascending=False)
        return wanted_df
    elif 'annee' in wanted_df.columns:
        wanted_df = stringyfy(wanted_df)
        return wanted_df
    else:
        return wanted_df



