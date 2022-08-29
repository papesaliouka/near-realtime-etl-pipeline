def filter_energie(df):
    if 'energie_active_secteur' in df.columns:
        df = df[(df['energie_active_secteur']>= df['energie_active_secteur'].quantile(0.25))&
            (df['energie_active_secteur']<= df['energie_active_secteur'].quantile(0.95))&
            (df['energie_reactive_secteur']>= df['energie_reactive_secteur'].quantile(0.25))&
            (df['energie_reactive_secteur']<= df['energie_reactive_secteur'].quantile(0.95))
            ]
        return df
    elif 'e_active' in df.columns:
        df = df[(df['e_active']>= df['e_active'].quantile(0.25))&
            (df['e_active']<= df['e_active'].quantile(0.95))&
            (df['e_reactive']>= df['e_reactive'].quantile(0.25))&
            (df['e_reactive']<= df['e_reactive'].quantile(0.95))
            ]
        return df
    else:
        return df

