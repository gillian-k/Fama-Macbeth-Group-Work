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
                  user='mingfan770')

crsp_query<- tbl(wrds, sql("select * from crsp.msf")) |>
  filter(date >= '1926-08-01' & date <= '1968-06-30') |>
  select(permno, date, ret) |> 
  collect()


crsp_query_msenames<- tbl(wrds, sql("select * from crsp.msenames")) |>
  select(permno, primexch, shrcd) |> collect() |> unique()

full_data <- crsp_query |> 
  left_join(
    crsp_query_msenames, 
    by = c('permno'), keep=FALSE
  ) |> 
  filter(primexch == 'N')|>
  filter(shrcd == 10)


full_data$year_month <- format(full_data$date, "%Y-%m")

#SECTION A - RISK FREE RATE FROM FAMA FRENCH
#####################################
temp <- tempfile(fileext = ".zip")
download.file("https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_CSV.zip",temp)
temp1 <- unzip(temp, exdir = ".")



ff_3factors_monthly <- read.csv(temp1, skip=5, header = F) 
names(ff_3factors_monthly) <- c('dt', 'rmrf', 'smb', 'hml', 'rf')
unlink(temp)
unlink(temp1)
head(ff_3factors_monthly)



ff_3factors_mon <- ff_3factors_monthly |> 
  filter(nchar(dt) == 6) |> 
  mutate(yr = str_sub(dt,1,4), mon= str_sub(dt,-2,-1),  
         date = make_date(year= yr, month = mon, day = 01),
         mkt_excess = as.numeric(rmrf), smb = as.numeric(smb),
         hml = as.numeric(hml), rf = as.numeric(rf)) |> 
  select(c('date','mkt_excess','smb','hml','rf'))
ff_3factors_mon <- as_tibble(ff_3factors_mon)



ff_3factors_mon$date <- as.Date(ff_3factors_mon$date)
ff_3factors_mon$date <- ceiling_date(ff_3factors_mon$date, "month") - days(1)
ff_3factors_mon$year_month <- format(ff_3factors_mon$date, "%Y-%m")



risk_free<-ff_3factors_mon|>
  select(rf, year_month)|>
  mutate(rf=rf/100)



#MERGE
full_data_with_rf <- full_data |> 
  left_join(
    risk_free, 
    by = c('year_month'), keep=FALSE)
########################################################



#SECTION B: Set up dataset in local DB



setwd("C:/Users/gkimu/OneDrive/Documents/GitHub/Fama-Macbeth-Group-Work")
Fama_Macbeth<- dbConnect(
  SQLite(),
  "data/FM_data.sqlite",
  extended_types = TRUE
)



dbWriteTable(Fama_Macbeth,
             "full_data", #name of table
             value = full_data, #content of table
             overwrite = TRUE )



dbListTables(Fama_Macbeth)





#SECTION C - Extract Data for each of the Periods
#SECTION C1 - Portfolio Formation  to Testing Period
period2 <- tbl(Fama_Macbeth, "full_data")|> 
  select(permno, date, ret, primexch, shrcd) |> 
  collect()|>
  mutate(date=as.Date(date, format="%Y-%m-%d"))|>
  mutate(month = floor_date(date, "month"), year=year(date))|>
  na.omit()|>
  group_by(month)|>
  mutate(mkt = mean(ret))|>
  filter(date>='1927-01-01' & date <= '1942-12-31')|>
  ungroup()





count1<- period2 %>% group_by(permno) %>%  #no of securities available 
  summarise(total_count=n(),
            .groups = 'drop')



#SECTION C2 - Portfolio Formation Period Data
crsp_pf_period2 <- tbl(Fama_Macbeth, "full_data")|> 
  select(permno, date, ret, primexch) |> 
  collect()|>
  mutate(date=ymd(date))|>
  mutate(month = floor_date(date, "month"), year=floor_date(date, "year"))|>
  na.omit()|>
  group_by(month)|>
  mutate(mkt = mean(ret))|>
  filter(date>='1927-01-01' & date<='1933-12-31')





#Create beta estimation function
estimate_capm <- function(data, min_obs) {
  if (nrow(data) < min_obs) {
    beta <- as.numeric(NA)
    sdres <- as.numeric(NA)
  } else {
    fit <- lm(ret~mkt, data = data)
    beta <- as.numeric(coefficients(fit)[2])
    sdres <- as.numeric(sd(residuals(fit), na.rm=TRUE))
  }
  return(tibble(
    beta,
    sdres))
}



#SECTION C2.2 - Estimation of Pre-Ranking Betas - Portfolio Formation Period
#Note: Minimum number of observation from period 2 onwards is now 4 years
beta_pf_period2 <- crsp_pf_period2 |>
  group_by(permno) |>
  mutate(pf=estimate_capm(pick(everything()), min_obs = 48)) |>##picks everything from last pipe and uses that as the dataset
  ungroup()|>
  drop_na()|>
  select(permno, pf) |>
  unique()|>
  arrange(pf$beta)|>
  mutate(port=as.numeric(cut_number(pf$beta,20)))|> 
  drop_na()
#The last line on "mutate" Uses "cut_number" to sort the betas into 20 portfolios, after sorting in ascending order


#SECTION C3 - Initial Estimation Period
crsp_ie_period2 <- tbl(Fama_Macbeth, "full_data")|> 
  select(permno, date, ret, primexch) |> 
  collect()|>
  mutate(date=as.Date(date, format="%Y-%m-%d"))|>
  mutate(month = floor_date(date, "month"), year=year(date))|>
  na.omit()|>
  group_by(month)|>
  mutate(mkt = mean(ret))|>
  filter(date>='1934-01-01' & date <= '1942-12-31')|>
  ungroup()





#SECTION C3.2 - Estimation of Betas in Estimation Period
#Note: Minimum number of observations is now 5 years



roll_capm_estimation <- function(data, months,  min_obs) {
  data <- data |> arrange(month)
  ie <- slide_period_vec(
    .x = data,
    .i = data$month,#specifies index
    .period = "month",
    .before= Inf,  #it will take all the current year and 'years' before
    .f = ~ estimate_capm(., min_obs), #earlier specified CAPM estimation function
    .complete = TRUE)# the function  evaluated on complete windows only
  return(tibble(
    estimation_end = unique(data$month),
    ie ))} #create tibble with month and CAPM beta



###Iterative Beta Estimation
##Estimations for multiple firms
## ------------1--------------Iterate using “group by(permno)”



beta_ie_period2 <- crsp_ie_period2 |>
  group_by(permno)|>
  mutate(beta_ie=roll_capm_estimation(pick(everything()), months=60, min_obs = 60))|>##picks everything from last pipe and uses that as the dataset
  ungroup()|>
  select(permno, beta_ie)|>
  drop_na()|>
  group_by(permno)|>
  mutate(date=ymd(beta_ie$estimation_end))|>
  mutate(date=ceiling_date(as.Date(date, format="%Y-%m-%d"), "month")-days(1))|>
  unique()


beta_ie_period2 <- bind_cols(beta_ie_period2[1], reduce(beta_ie_period2[-1], data.frame))



beta_ie_period2 <- beta_ie_period2 |> rename(date=elt)



beta_ie_period2 <-beta_ie_period2 |>
  filter(month(date)==12) #This keeps beta calculated as at end of every year, 
#This is because beta in estimation window is updated every year according to the paper.



###############################################
