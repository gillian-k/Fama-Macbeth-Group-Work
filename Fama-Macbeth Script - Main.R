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
library(slider)
#.x = data input
#~.x = function parsed on data x

wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='kgillian')

crsp_query<- tbl(wrds, sql("select * from crsp.msf")) |>
  filter(date >= '1926-08-01' & date <= '1968-06-30') |>
  select(permno, date, ret) |> collect()


crsp_query_msenames<- tbl(wrds, sql("select * from crsp.msenames")) |>
  select(permno, primexch, shrcd) |> collect() |> unique()

full_data <- crsp_query |> 
  left_join(
    crsp_query_msenames, 
    by = c('permno'), keep=FALSE
  ) |> 
  filter(primexch == 'N')|>
  filter(shrcd == 10)