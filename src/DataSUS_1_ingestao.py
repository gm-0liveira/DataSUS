# Databricks notebook source
import urllib.request
from tqdm import tqdm
from multiprocessing import Pool

import sys
sys.path.insert(0, '../libs')
import dttools

# COMMAND ----------

def get_data_uf_ano_mes(uf, ano, mes):
  url = f"ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/RD{uf}{ano}{mes}.dbc"
  path = f"/Users/gm.oliveira@unesp.br/DataSUS/data/dbc/RD{uf}{ano}{mes}.dbc"

  resp = urllib.request.urlretrieve(url, path)

def get_data_uf(uf, datas):
  for i in tqdm(datas):
    ano, mes, dia = i.split('-')
    ano = ano[-2:]
    get_data_uf_ano_mes(uf, ano, mes)

ufs = ['RO', 'AC', 'AM', 'RR', 'PA', 'AP', 'TO', 'MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE',
       'BA', 'MG', 'ES', 'RJ', 'SP', 'PR', 'SC', 'RS', 'MS', 'MT', 'GO', 'DF']

dt_start = dbutils.widgets.get('dt_start')
dt_stop = dbutils.widgets.get('dt_stop')

datas = dttools.date_range(dt_start, dt_stop, monthly=True)
to_download = [(uf, datas) for uf in ufs]

# COMMAND ----------

with Pool(8) as pool:
  pool.starmap(get_data_uf, to_download)

# COMMAND ----------


