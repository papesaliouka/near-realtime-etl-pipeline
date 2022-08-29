def filter_puissance(df):
    if ('cos_phi' in df.columns) & ('tension_1_mesuree'in df.columns):
        df['p_1']=df['cos_phi']*df['intensite_1_mesuree']*df['tension_1_mesuree']
        df['p_2']=df['cos_phi']*df['intensite_2_mesuree']*df['tension_2_mesuree']
        df['p_3']=df['cos_phi']*df['intensite_3_mesuree']*df['tension_3_mesuree']
        df['puissance_mesuree'] = df['p_1']+df['p_2']+df['p_3']
        return df
    else:
        df = df[
                (df['p_1']>=df['p_1'].quantile(0.25))& (df['p_1']<=df['p_1'].quantile(0.95))&
                (df['p_2']>=df['p_2'].quantile(0.25))& (df['p_2']<=df['p_2'].quantile(0.95))&
                (df['p_3']>=df['p_3'].quantile(0.25))& (df['p_3']<=df['p_3'].quantile(0.95))
                ]
        return df
