from utils import *
from archiver.index import *
from calcul_conso.index import *
from calcul_stats.index import *
from filtrages.apply_facteur import *
from filtrages.index import *
from headers import *

def func(wanted_df,redis_key,path):

    if len(wanted_df.columns)==29:
        wanted_df.columns=header_box
    elif len(wanted_df.columns) ==27:
        wanted_df.columns= header_onas
    elif len(wanted_df.columns) == 28:
        wanted_df.columns = header_standard
    else:
        pass
    
    wanted_df = make_datetime(wanted_df)
    
    complement = getComplement(redis_key)
   
    wanted_df = add_complement(complement,wanted_df)
    
    wanted_df = apply_facteur(wanted_df)
    
    wanted_df = apply_filter(wanted_df)

    energie_hour_array = get_energies(wanted_df)

    stats = get_stats(wanted_df,energie_hour_array)

    insert_to_stats(stats)

    insert_to_releves(wanted_df)

    archiver(path, path.replace('ftp','ftp-archive'),redis_key.replace('ftp','ftp-archive'))

def start():
    try:
        path,redis_key = getPathAndRedisKey()
        files = ps.read_csv(path)
        func(files,redis_key,path)
    except:
        raise 'unable to process file'

if __name__=='__main__':
    start()
