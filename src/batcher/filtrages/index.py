from filtrages.filter_cosphi import filter_cosphi
from filtrages.filter_tensions import *
from filtrages.filter_thd import *
from filtrages.filtrer_intensites import *
from filtrages.filter_puissances import *
from filtrages.filter_energie import *

def apply_filter(wanted_df):
    try:
        try:
            wanted_df = filter_cosphi(wanted_df)
        except:
            print('error filtering cos_phi')
        try:
            wanted_df = filter_energie(wanted_df)
        except:
            print('error filtering energie')
        try:
            wanted_df = filter_tension(wanted_df)
        except:
            print('error filtering tensions')
        try: 
            wanted_df= filter_thd(wanted_df)
        except:
            print('error filtering thd')
        try:
            wanted_df = filter_intensite(wanted_df)
        except:
            print('error filtering intensite')
        try:
            wanted_df = filter_puissance(wanted_df)
        except:
            print('error filtering puissance')
        return wanted_df
    except:
        print('error when filtering')
        raise 'filter error'

