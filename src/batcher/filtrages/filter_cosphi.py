def filter_cosphi(df):
    if not 'cos_phi' in df.columns:
        return df
    else:
        df=df[(df['cos_phi']>=0) & (df['cos_phi']<=1000)]
        return df
