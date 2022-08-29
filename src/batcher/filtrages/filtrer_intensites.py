def filter_intensite(df):
    if ('u_l1' in df.columns) & ('intensite_1_mesuree' in df.columns) :
        df = df[
                (df['intensite_1_mesuree']<= df['puissance_souscrite'] * df['facteur_p']  / df['u_l1'] *1.72)&
                (df['intensite_2_mesuree']<= df['puissance_souscrite'] * df['facteur_p'] / df['u_l2'] *1.72)&
                (df['intensite_3_mesuree']<= df['puissance_souscrite'] * df['facteur_p'] / df['u_l3'] *1.72)
                ]
        return df
    elif ('u_l1' in df.columns) & ('i_1' in df.columns) :
        df = df[
                (df['i_1']<= df['puissance_souscrite'] * df['facteur_p']  / df['u_l1'] *1.72)&
                (df['i_2']<= df['puissance_souscrite'] * df['facteur_p'] / df['u_l2'] *1.72)&
                (df['i_3']<= df['puissance_souscrite'] * df['facteur_p'] / df['u_l3'] *1.72)
                ]
        return df
    elif ('tension_1_mesuree' in df.columns) & ('intensite_1_mesuree' in df.columns):
        df = df[
                (df['intensite_1_mesuree']<= df['puissance_souscrite'] * df['facteur_p'] / df['tension_1_mesuree'] *1.72)&
                (df['intensite_2_mesuree']<= df['puissance_souscrite'] * df['facteur_p'] / df['tension_2_mesuree'] *1.72)&
                (df['intensite_3_mesuree']<= df['puissance_souscrite'] * df['facteur_p'] / df['tension_3_mesuree'] *1.72)
                ]
        return df
    elif ('i_1' in df.columns) & ('tension_1_mesuree' in df.columns):
        df = df[
                (df['i_1']<= df['puissance_souscrite'] * df['facteur_p'] / df['tension_1_mesuree'] *1.72)&
                (df['i_2']<= df['puissance_souscrite'] * df['facteur_p'] / df['tension_2_mesuree'] *1.72)&
                (df['i_3']<= df['puissance_souscrite'] * df['facteur_p'] / df['tension_3_mesuree'] *1.72)
                ]
        return df
    else:
        pass
    if ('i' in df.columns):
        df = df[(df['i']>= df['i'].quantile(0.25))& (df['i']<= df['i'].quantile(0.95))]
        return df
    else:
        pass
    return df
