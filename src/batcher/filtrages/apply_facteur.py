def apply_facteur(wanted_df):
    try:
        print('applying facteur')
        if 'i' in wanted_df.columns:
            wanted_df['i'] = wanted_df['i']/wanted_df['facteur_i']
        else:
            pass
        if 'puissance_reactive' in wanted_df.columns:
            wanted_df['puissance_reactive'] = wanted_df['puissance_reactive']/wanted_df['facteur_p']
        else:
            pass
        if 'intensite' in wanted_df.columns:
            wanted_df['intensite'] = wanted_df['intensite']/wanted_df['facteur_i']
        else:
            pass
        if 'i_1' in wanted_df.columns:
            wanted_df['i_1'] = wanted_df['i_1']/wanted_df['facteur_i']
            wanted_df['i_2'] = wanted_df['i_2']/wanted_df['facteur_i']
            wanted_df['i_3'] = wanted_df['i_3']/wanted_df['facteur_i']
        else:
            pass
        
        if 'u_1' in wanted_df.columns:
            wanted_df['u_1'] = wanted_df['u_1']/wanted_df['facteur_p']
            wanted_df['u_2'] = wanted_df['u_2']/wanted_df['facteur_p']
            wanted_df['u_3'] = wanted_df['u_3']/wanted_df['facteur_p']
        else:
            pass

        if 'i_neutre' in wanted_df.columns:   
            wanted_df['i_neutre'] = wanted_df['i_neutre']/wanted_df['facteur_i']
        else:
            pass

        if 'p_1' in wanted_df.columns:
            wanted_df['p_1'] = wanted_df['p_1']/wanted_df['facteur_p']
            wanted_df['p_2'] = wanted_df['p_2']/wanted_df['facteur_p']
            wanted_df['p_3'] = wanted_df['p_3']/wanted_df['facteur_p']
        else:
            pass

        if 'puissance_mesuree' in wanted_df.columns:
            wanted_df['puissance_mesuree'] = wanted_df['puissance_mesuree']/wanted_df['facteur_p']
        else:
            pass
        if 'u_l1' in wanted_df.columns:
            wanted_df['u_l1'] = wanted_df['u_l1']/wanted_df['facteur_p']
            wanted_df['u_l2'] = wanted_df['u_l2']/wanted_df['facteur_p']
            wanted_df['u_l3'] = wanted_df['u_l3']/wanted_df['facteur_p']
        else:
            pass
        if 'intensite_1_mesuree' in wanted_df.columns:
            wanted_df['intensite_1_mesuree'] = wanted_df['intensite_1_mesuree']/wanted_df['facteur_i']
            wanted_df['intensite_2_mesuree'] = wanted_df['intensite_2_mesuree']/wanted_df['facteur_i']
            wanted_df['intensite_3_mesuree'] = wanted_df['intensite_3_mesuree']/wanted_df['facteur_i']
        else:
            pass
        if 'tension_1_mesuree' in wanted_df.columns:
            wanted_df['tension_1_mesuree'] = wanted_df['tension_1_mesuree']/wanted_df['facteur_p']
            wanted_df['tension_2_mesuree'] = wanted_df['tension_2_mesuree']/wanted_df['facteur_p']
            wanted_df['tension_3_mesuree'] = wanted_df['tension_3_mesuree']/wanted_df['facteur_p']
        else:
            pass
        if 'thd_u1' in wanted_df.columns:
            wanted_df['thd_u1'] = wanted_df['thd_u1']/wanted_df['facteur_p']
            wanted_df['thd_u2'] = wanted_df['thd_u2']/wanted_df['facteur_p']
            wanted_df['thd_u3'] = wanted_df['thd_u3']/wanted_df['facteur_p']
            wanted_df['thd_i1'] = wanted_df['thd_i1']/wanted_df['facteur_p']
            wanted_df['thd_i2'] = wanted_df['thd_i2']/wanted_df['facteur_p']
            wanted_df['thd_i3'] = wanted_df['thd_i3']/wanted_df['facteur_p']
        else:
            pass
        if 'cos_phi' in wanted_df.columns:
            wanted_df['cos_phi'] = wanted_df['cos_phi']/wanted_df['facteur_p']
        else:
            pass
        print('facteur applied')
        return wanted_df
    except:
        print('enable to apply facteur')
        raise 'facteur error'
