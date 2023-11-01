# Databricks notebook source
library(read.dbc)

dbc_folder <- '../data/dbc'
csv_folder <- '../data/csv/'

files <- list.files(dbc_folder, full.names=TRUE)
for(f in files) {
    df = read.dbc(f)
    lista = strsplit(f, '/')[[1]]
    file = gsub('.dbc', '.csv', lista[length(lista)])
    write.csv2(df, paste(csv_folder, file, sep='/'))
}

# COMMAND ----------


