from calcul_conso.index import *
def get_infos_for_stats(df):
    site=df.site[0]
    id_compteur = df.id_compteur[0]
    id_depart_reel = df.id_depart_reel[0]
    return {'site':site,'id_compteur':id_compteur, 'id_depart_reel':id_depart_reel}

def get_stats(df,energie_hour_array):
    stats = []
    autres = get_infos_for_stats(df);
    for _,(energie,heure) in enumerate(energie_hour_array):
        montant = calcul_montant(df,energie,heure.hour)
        stats.append({**{'energie':energie,'heure':heure,'montant':montant},**autres})
    return stats

def make_stats(df):
    intensite_moyen = df.i.aggregate(np.mean)
    intensite_max = df.i.aggregate(np.max)
    intensite_min = df.i.aggregate(np.min)
    tension_moyen = df.ul_1.aggregate(np.min)
    tension_min = df.ul_1.aggregate(np.min)
    tension_max = df.ul_1.aggregate(np.max)
    puissance_maxi= df.p_1.aggregate(np.max)
    puissance_min = df.p_1.aggregate(np.min)
    puissance_moyen = df.p_1.aggregate(np.mean)
    return (intensite_min,intensite_max,intensite_moyen,tension_min,tension_max,tension_moyen,puissance_maxi,puissance_min,puissance_moyen) 
