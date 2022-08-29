def get_energies(df):
    print(df)
    if 'e_active' in df.columns:
        print('here')
        energie = df.resample('H').agg({'e_active':'diff'})
        energie.reset_index(inplace=True)
        return list(zip(energie['e_active'].values.tolist(),energie['datetime'].tolist()))
    else:
        print('second')
        print(df.energie_active_secteur)
        energie =  df.resample('H').agg({'energie_active_secteur':'diff'})
        energie.reset_index(inplace=True)
        return list(zip(energie['energie_active_secteur'].values.tolist(),energie['datetime'].tolist()))

def calcul_montant(df,energie,heure):
    code = df.code[0]
    tarif_1er_tranche = df.tarif_1er_tranche[0]
    tarif_2eme_tranche = df.tarif_2eme_tranche[0]
    tarif_3eme_tranche = df.tarif_3eme_tranche[0]
    tarif_heure_pleine = df.tarif_heure_pleine[0]
    tarif_heure_creuse = df.tarif_heure_creuse[0]
    max_1er_tranche = df.max_1er_tranche[0]
    max_2eme_tranche = df.max_2eme_tranche[0]
    facteur_c = df.facteur_c[0]
    id_compteur = df.id_compteur[0]
    if code == 'GP':
        if (heure >=19) & (heure<=23):
            montant = energie /facteur_c * tarif_heure_pleine
            return montant
        else:
            montant  = energie /facteur_c * tarif_heure_creuse
            return montant
    else:
        # get redis energie we will call it conso_redis
        conso_redis = redis.get(id_compteur)
        conso_total_mois_en_cours = conso_redis + energie
        if conso_total_mois_en_cours < max_1er_tranche:
            montant = energie /facteur_c * tarif_1er_tranche
            return montant
        elif (conso_total_mois_en_cours >=max_1er_tranche) & (conso_total_mois_en_cours<=max_2eme_tranche):
            montant = energie /facteur_c * tarif_2eme_tranche
            return montant
        else:
            montant = energie/facteur_c*tarif_3eme_tranche
            return montant
