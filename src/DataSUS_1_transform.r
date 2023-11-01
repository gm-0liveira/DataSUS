# Databricks notebook source
install.packages('read.dbc')

# COMMAND ----------

library(read.dbc)

dbc_folder <- '../../../../Users/gm.oliveira@unesp.br/DataSUS/data/dbc'
csv_folder <- '../../../../Users/gm.oliveira@unesp.br/DataSUS/data/csv'

files <- list.files(dbc_folder, full.names=TRUE)
for(f in files) {
    print(f)
    df = read.dbc(f)
    lista = strsplit(f, '/')[[1]]
    file = gsub('.dbc', '.csv', lista[length(lista)])
    write.csv2(df, paste(csv_folder, file, sep='/'))
}

# COMMAND ----------


