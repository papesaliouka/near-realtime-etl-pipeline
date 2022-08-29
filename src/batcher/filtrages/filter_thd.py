def filter_thd(wanted_df):
    wanted_df = wanted_df[(wanted_df['thd_u1']>=wanted_df['thd_u1'].quantile(0.025))&
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
            (wanted_df['thd_i3']<=wanted_df['thd_i3'].quantile(0.95))
            ]
    return wanted_df

