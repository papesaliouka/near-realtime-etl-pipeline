import pandas as ps
from pymongo import MongoClient
import json
import sys
import redis
import functools


client = MongoClient('localhost',27017)
db = client['sentinel-dev']

r = redis.Redis(host='localhost', port=6379)


header_standard=['jour','mois','annee','heure','minute','seconde','energie_active_secteur','energie_reactive_secteur','puissance_mesuree','puissance_reactive','intensite_1_mesuree','intensite_2_mesuree','intensite_3_mesuree','intensite','u_l1','u_l2','u_l3','p_1','p_2','p_3','thd_u1','thd_u2','thd_u3','thd_i1','thd_i2','thd_i3','xxx','i_neutre'];

try:
    path = functools.reduce( lambda x,y: x+' '+y ,sys.argv[1:])
    poped = path.split('/').pop()
    redis_key = path.replace(f"/{poped}",'')
    print('filepath',path)
    print('redis-key',redis_key)
except:
    print('file not found or invalid key')

try:
    complement = json.loads(r.get(redis_key))
    print('redis data found',complement)
except:
    print('error unable to find redis data')

    
def func(wanted_df):
    wanted_df.columns=header_standard
    try:
        for _,(key,value) in enumerate(complement.items()):
            wanted_df[key]=value
        print('added redis data to dataset')
    except:
        print('redis data not found') 
    try:
        wanted_df = wanted_df[( wanted_df['energie_active_secteur'] >= wanted_df['energie_active_secteur'].quantile(0.025))& 
            ( wanted_df['energie_active_secteur'] <= wanted_df['energie_active_secteur'].quantile(0.95))&
            (wanted_df['energie_reactive_secteur']<=wanted_df['energie_reactive_secteur'].quantile(0.95))&
            (wanted_df['energie_reactive_secteur']>=wanted_df['energie_reactive_secteur'].quantile(0.025))&
            (wanted_df['puissance_mesuree']>=wanted_df['puissance_mesuree'].quantile(0.025))&
            (wanted_df['puissance_mesuree']<=wanted_df['puissance_mesuree'].quantile(0.95))&
            (wanted_df['puissance_reactive']>=wanted_df['puissance_reactive'].quantile(0.025))&
            (wanted_df['puissance_reactive']<=wanted_df['puissance_reactive'].quantile(0.95))&
            (wanted_df['intensite_1_mesuree']>=wanted_df['intensite_1_mesuree'].quantile(0.025))&
            (wanted_df['intensite_1_mesuree']<=wanted_df['intensite_1_mesuree'].quantile(0.95))&
            (wanted_df['intensite_2_mesuree']>=wanted_df['intensite_2_mesuree'].quantile(0.025))&
            (wanted_df['intensite_2_mesuree']<=wanted_df['intensite_2_mesuree'].quantile(0.95))&
            (wanted_df['intensite_3_mesuree']>=wanted_df['intensite_3_mesuree'].quantile(0.025))&
            (wanted_df['intensite_3_mesuree']<=wanted_df['intensite_3_mesuree'].quantile(0.95))&
            (wanted_df['intensite']>=wanted_df['intensite'].quantile(0.025))&
            (wanted_df['intensite']<=wanted_df['intensite'].quantile(0.95))&
            (wanted_df['u_l1']>=wanted_df['u_l1'].quantile(0.025))&
            (wanted_df['u_l1']<=wanted_df['u_l1'].quantile(0.95))&
            (wanted_df['u_l2']>=wanted_df['u_l2'].quantile(0.025))&
            (wanted_df['u_l2']<=wanted_df['u_l2'].quantile(0.95))&
            (wanted_df['u_l3']>=wanted_df['u_l3'].quantile(0.025))&
            (wanted_df['u_l3']<=wanted_df['u_l3'].quantile(0.95))&
            (wanted_df['p_1']>=wanted_df['p_1'].quantile(0.025))&
            (wanted_df['p_1']<=wanted_df['p_1'].quantile(0.95))&
            (wanted_df['p_2']>=wanted_df['p_2'].quantile(0.025))&
            (wanted_df['p_2']<=wanted_df['p_2'].quantile(0.95))&
            (wanted_df['p_3']>=wanted_df['p_3'].quantile(0.025))&
            (wanted_df['p_3']<=wanted_df['p_3'].quantile(0.95))&
            (wanted_df['thd_u1']>=wanted_df['thd_u1'].quantile(0.025))&
            (wanted_df['thd_u1']<=wanted_df['thd_u1'].quantile(0.95))&
            (wanted_df['thd_u2']>=wanted_df['thd_u2'].quantile(0.025))&
            (wanted_df['thd_u2']<=wanted_df['thd_u2'].quantile(0.95))&
            (wanted_df['thd_u3']>=wanted_df['thd_u3'].quantile(0.025))&
            (wanted_df['thd_u3']<=wanted_df['thd_u3'].quantile(0.95))&
            (wanted_df['thd_i1']>=wanted_df['thd_i1'].quantile(0.025))&
            (wanted_df['thd_i1']<=wanted_df['thd_i1'].quantile(0.95))&
            (wanted_df['thd_i2']>=wanted_df['thd_i2'].quantile(0.025))&
            (wanted_df['thd_i2']<=wanted_df['thd_i2'].quantile(0.95))&
            (wanted_df['thd_i3']>=wanted_df['thd_i3'].quantile(0.025))&
            (wanted_df['thd_i3']<=wanted_df['thd_i3'].quantile(0.95))&
             (wanted_df['i_neutre']>=wanted_df['i_neutre'].quantile(0.025))&
            (wanted_df['i_neutre']<=wanted_df['i_neutre'].quantile(0.95))
            ]

        print('data filtered successfully')
    except:
        print('error when filtering')
    try:
        records = wanted_df.to_dict('records')
        db['test-pyspark14'].insert_many(records)
        print('data inserted successfullly to mongo')
    except:
        print('insertion error')
        
try:
    files = ps.read_csv(path)
    func(files)
except:
    print('something is wrong')
