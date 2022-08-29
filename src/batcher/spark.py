import findspark
findspark.init()
#!pip install pymongo

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,StringType, IntegerType, DateType
from pymongo import MongoClient
import json
import pathlib
import os
import sys
from utils import *
from archiver.index import *
from calcul_conso.index import *
from calcul_stats.index import *
from filtrages.apply_facteur import *
from filtrages.index import *
from headers import *
import pyspark.pandas as ps

client = MongoClient('localhost',27017)
db = client['sentinel-dev']


def func(batch_df,batchid):
    wanted_df = ps.DataFrame(batch_df.toPandas())
       
    complement = getComplement('/home/pape/sentinel-hybrid/ftp/ONAS/ONAS TECHNOPOLE/01')
   
    wanted_df = add_complement(complement,wanted_df)
    
    wanted_df = apply_facteur(wanted_df)
    
    wanted_df= apply_filter(wanted_df)
    
    energie_hour_array = get_energies(wanted_df)

    stats = get_stats(wanted_df,energie_hour_array)
    insert_to_stats(stats)
    
    insert_to_mongo(wanted_df)
    
    archiver('/home/pape/sentinel-hybrid/ftp/ONAS/ONAS TECHNOPOLE/05','/home/pape/sentinel-hybrid/ftp-archive/ONAS/ONAS TECHNOPOLE/05','/home/pape/sentinel-hybrid/ftp-archive/ONAS/ONAS TECHNOPOLE/05')
    
sc = SparkSession.builder.master('local[*]').appName('Test8').getOrCreate()
schema_standard = StructType([     StructField('jour', IntegerType(),True),     StructField('mois', IntegerType(),True),     StructField('annee', IntegerType(),True),     StructField('heure', IntegerType(),True),     StructField('minutes', IntegerType(),True),     StructField('secondes', IntegerType(),True),     StructField('energie_active_secteur', IntegerType(),True),     StructField('energie_reactive_secteur', IntegerType(),True),     StructField('puissance_mesuree', IntegerType(),True),     StructField('puissance_reactive', IntegerType(),True),     StructField('intensite_1_mesuree', IntegerType(),True),     StructField('intensite_2_mesuree', IntegerType(),True),     StructField('intensite_3_mesuree', IntegerType(),True),     StructField('intensite', IntegerType(),True),     StructField('u_l1', IntegerType(),True),     StructField('u_l2', IntegerType(),True),     StructField('u_l3', IntegerType(),True),     StructField('p_1', IntegerType(),True),     StructField('p_2', IntegerType(),True),     StructField('p_3', IntegerType(),True),     StructField('thd_u1', IntegerType(),True),     StructField('thd_u2', IntegerType(),True),     StructField('thd_u3', IntegerType(),True),     StructField('thd_i1', IntegerType(),True),     StructField('thd_i2', IntegerType(),True),     StructField('thd_i3', IntegerType(),True),     StructField('etat', IntegerType(),True),     StructField('i_neutre', IntegerType(),True)
 ])

schema_onas = StructType([
    StructField('date', StringType(),True), \
    StructField('heure',StringType(),True), \
    StructField('e_active', IntegerType(),True), \
    StructField('e_reactive',IntegerType(),True), \
    StructField('p_active', IntegerType(),True), \
    StructField('p_reactive',IntegerType(),True), \
    StructField('i_1', IntegerType(),True), \
    StructField('i_2',IntegerType(),True), \
    StructField('i_3',IntegerType(),True), \
    StructField('i',IntegerType(),True), \
    StructField('u_1', IntegerType(),True), \
    StructField('u_2',IntegerType(),True), \
    StructField('u_3',IntegerType(),True), \
    StructField('p_1', IntegerType(),True), \
    StructField('p_2',IntegerType(),True), \
    StructField('p_3',IntegerType(),True), \
    StructField('cos_phi', IntegerType(),True), \
    StructField('u_l1', IntegerType(),True), \
    StructField('u_l2',IntegerType(),True), \
    StructField('u_l3',IntegerType(),True), \
    StructField('thd_u1', IntegerType(),True), \
    StructField('thd_u2',IntegerType(),True), \
    StructField('thd_u3',IntegerType(),True), \
    StructField('thd_i1', IntegerType(),True), \
    StructField('thd_i2',IntegerType(),True), \
    StructField('thd_i3',IntegerType(),True), \
    StructField('i_neutre',IntegerType(),True), 
])

schema_box = StructType([
    StructField('jour', IntegerType(),True),  \
    StructField('mois', IntegerType(),True),  \
    StructField('annee', IntegerType(),True), \
    StructField('heure', IntegerType(),True), \
    StructField('minutes', IntegerType(),True), \
    StructField('secondes', IntegerType(),True), \
    StructField('energie_active_secteur', IntegerType(),True), \
    StructField('energie_reactive_secteur', IntegerType(),True), \
    StructField('puissance_mesuree', IntegerType(),True), \
    StructField('puissance_reactive', IntegerType(),True), \
    StructField('intensite_1_mesuree', IntegerType(),True), \
    StructField('intensite_2_mesuree', IntegerType(),True), \
    StructField('intensite_3_mesuree', IntegerType(),True), \
    StructField('intensite', IntegerType(),True), \
    StructField('tension_1_mesuree', IntegerType(),True), \
    StructField('tension_2_mesuree', IntegerType(),True), \
    StructField('tension_3_mesuree', IntegerType(),True), \
    StructField('p_1', IntegerType(),True), \
    StructField('p_2', IntegerType(),True), \
    StructField('p_3', IntegerType(),True), \
    StructField('cos_phi', IntegerType(),True), \
    StructField('u_l1', IntegerType(),True), \
    StructField('u_l2', IntegerType(),True), \
    StructField('u_l3', IntegerType(),True), \
    StructField('thd_u1', IntegerType(),True), \
    StructField('thd_u2', IntegerType(),True), \
    StructField('thd_u3', IntegerType(),True), \
    StructField('thd_i1', IntegerType(),True), \
    StructField('thd_i2', IntegerType(),True), \
    StructField('thd_i3', IntegerType(),True), \
    StructField('i_neutre', IntegerType(),True), \
    StructField('site', StringType(),True), \
    StructField('i_neutre', IntegerType(),True)
 ])
ssc = sc.readStream.format('csv').option('path',"/home/pape/Downloads/onas/01").schema(schema_onas).load()
ssc.writeStream.format('console').foreachBatch(func).outputMode('append').start().awaitTermination()
