library(tidyverse)
library(lubridate)
library(readxl)
library(RPostgres)
library(RSQLite)
library(dbplyr)
library(furrr)
library(slider)
library(purrr)
library(ggplot2)

wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='kgillian')