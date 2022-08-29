def filter_tension_simple(df):
    if 'u_l1' in df.columns:
        df = df[(df['u_l1']>=0) & (df['u_l1']<=500) &(df['u_l2']>=0) & (df['u_l2']<=500)&(df['u_l3']>=0) & (df['u_l3']<=500)]
    elif 'tension_1_mesuree' in df.columns:
        df = df[(df['tension_1_mesuree']>=0) & (df['tension_1_mesuree']<=500)&(df['tension_2_mesuree']>=0) & (df['tension_2_mesuree']<=500)&(df['tension_3_mesuree']>=0) & (df['tension_3_mesuree']<=500)]
        return df
    else:
        pass

    if 'u_1' in df.columns:
        df = df[(df['u_1']<=500) & (df['u_1']>=0)&(df['u_2']<=500) & (df['u_2']>=0)&(df['u_3']<=500) & (df['u_3']>=0) ]
        return df
    else:
        return df


def filter_tension_composee(df):
    if 'u_l1' in df.columns:
        df = df[(df['u_l1']>=0) & (df['u_l1']<=500) &(df['u_l2']>=0) & (df['u_l2']<=500)&(df['u_l3']>=0) & (df['u_l3']<=500)]
    elif 'tension_1_mesuree' in df.columns:
        df = df[(df['tension_1_mesuree']>=0) & (df['tension_1_mesuree']<=500)&(df['tension_2_mesuree']>=0) & (df['tension_2_mesuree']<=500)&(df['tension_3_mesuree']>=0) & (df['tension_3_mesuree']<=500)]
    return df


def filter_tension(df):
    df = filter_tension_simple(df)
    df = filter_tension_composee(df)
    return df
