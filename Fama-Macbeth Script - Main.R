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


#start by merging stock return data with estimated betas, and portfolio placement
returns_period2<-crsp_ie_period2|>
  select(permno, date, ret, mkt)|>
  mutate(beta_year=(year(date))-1)





stock_betas_period2 <- beta_ie_period2 |> #merge between estimation betas and portfolio placement
  inner_join((beta_pf_period2|>
                select(permno, port)), 
             by = c('permno'),keep=FALSE)|>
  mutate(beta_year=(year(date)))|>
  select(-date)





stock_data_period2<-returns_period2|>
  inner_join(stock_betas_period2, by =c('permno', 'beta_year'))|>
  mutate(beta_sq=beta^2) #page 616 - squared values of beta at stock level





count2<- stock_data_period2 %>% group_by(permno) %>%  #no of securities meeting data requirement for portfolio formation and initial estimation period
  summarise(total_count=n(),
            .groups = 'drop')



#Create portfolio dataframe with average returns, average betas, average standard deviation of residuals



port_data_period2 <- stock_data_period2 |> 
  group_by(date, port)|>
  summarise(across(
    .cols = c(ret, mkt, beta, beta_sq, sdres),
    .fns = list(Port_Mean = mean, Port_Sd=sd), na.rm = TRUE, 
    .names = "{col}_{fn}"))|>
  ungroup()





#For the regression we need three things:
##---->the portfolio beta (average of stock betas)
##---->the square portfolio beta - average of squared stock betas
##---->the \bar(sp_t-1(eps_i)) which is the portfolio average of 'sdres' measured at stock level

#SECTION E: CONSTRUCT TABLE 2



table2_part_a <- port_data_period2|>
  group_by(port) |>
  summarise(beta_p=mean(beta_Port_Mean),sp_t=mean(sdres_Port_Mean),srp=sd(ret_Port_Mean), sdmkt=sd(mkt_Port_Mean))



#Create beta estimation function for portfolios



estimate_capm_port <- function(data, min_obs) {
  if (nrow(data) < min_obs) {
    se_p <- as.numeric(NA)
  } else {
    fit <- lm(ret_Port_Mean~mkt_Port_Mean, data = data)
    rsquare <- summary(fit)$r.squared
    se_p <- as.numeric(sd(residuals(fit), na.rm=TRUE))
  }
  return(tibble(
    se_p,
    rsquare))
}





table2_part_b <- port_data_period2 |>
  mutate(month= floor_date(date, "month"))|>
  group_by(port)|>
  mutate(res_ie=estimate_capm_port(pick(everything()),min_obs = 48))|>##picks everything from last pipe and uses that as the dataset
  ungroup()|>
  select(port, res_ie)|>
  drop_na()|>
  group_by(port)|>
  unique()



table2_part_b <- bind_cols(table2_part_b[1], reduce(table2_part_b[-1], data.frame))



table2 <- table2_part_a|>
  inner_join(
    table2_part_b, 
    by = c('port'), keep=FALSE)|>
  mutate(se_beta=se_p/(sqrt(48)*sdmkt),ratio=se_p/sp_t)|>
  t()



colnames(table2) <- as.character(table2[1, ])
table2 <- table2[-1,]


#SECTION F: CONSTRUCT TABLE 3



#THIS IS THE STACKED DATASET WITH ALL PORTFOLIO DATA
stacked_data <- read_csv("stacked_data_cross_sectional_regression.csv")





#Create LAMBDA estimation function (FULL MODEL) - risk premium is called lambda
estimate_lambdas <- function(data, min_obs) {
  if (nrow(data) < min_obs) {
    lambda0 <- as.numeric(NA)
    lambda1 <- as.numeric(NA)
    lambda2 <- as.numeric(NA)
    lambda3 <- as.numeric(NA)
    r_square <- as.numeric(NA)
  } else {
    fit <- lm(ret_Port_Mean~beta_Port_Mean+beta_sq_Port_Mean+sdres_Port_Mean, data = data)
    lambda0 <- as.numeric(coefficients(fit)[1])
    lambda1 <- as.numeric(coefficients(fit)[2])
    lambda2 <- as.numeric(coefficients(fit)[3])
    lambda3 <- as.numeric(coefficients(fit)[4])
    rsquare <- summary(fit)$r.squared
  }
  return(tibble(
    lambda0,
    lambda1,
    lambda2,
    lambda3,
    rsquare))
}



lambdas <- stacked_data |>
  group_by(date) |>
  mutate(model=estimate_lambdas(pick(everything()), min_obs=0)) |>##picks everything from last pipe and uses that as the dataset
  ungroup() |>
  select(date, subperiodlarge, subperiodsmall, model) |>
  drop_na()|>
  unique()



lambdas <- bind_cols(lambdas[1], reduce(lambdas[-1], data.frame))
lambdas$date <- as.Date(lambdas$date, "%d/%m/%Y")
lambdas$year_month<-format(lambdas$date, "%Y-%m")



#OUTPUT IS DATAFRAME CALLED LAMBDAS



#COMBINE RISK FREE RATE DATA WITH THE LAMBDAS
lambdas_with_rf <- lambdas |> 
  left_join(
    risk_free, 
    by = c('year_month'), keep=FALSE)|>
  mutate(lambda0rf=lambda0-rf) #This is lambda_0 minus the risk free rate, as seen in Table 3 of paper





#SECION F2 - TABLE 3 SUMMARIES



#SUMMARY 1A - AVERAGE AND STANDARD DEVIATION OF LAMBDAS
summary1A<-lambdas_with_rf |>
  group_by(out)|>
  summarise(across(
    .cols = c(lambda0, lambda1, lambda2, lambda3, lambda0rf, rsquare), 
    .fns = list(Mean = mean, SD = sd),  
    .names = "{col}_{fn}"))



#SUMMARY 1B - LONG RANGE WINDOWS - AVERAGE AND STANDARD DEVIATION OF LAMBDAS
summary1B<-lambdas_with_rf |>
  group_by(elt)|>
  summarise(across(
    .cols = c(lambda0, lambda1, lambda2, lambda3, lambda0rf, rsquare), 
    .fns = list(Mean = mean, SD = sd),  
    .names = "{col}_{fn}"))



#SUMMARY 1C - SHORTER RANGE WINDOWS - AVERAGE AND STANDARD DEVIATION OF LAMBDAS
summary1C<-lambdas_with_rf |>
  group_by(elt)|>
  summarise(across(
    .cols = c(lambda0, lambda1, lambda2, lambda3, lambda0rf, rsquare), 
    .fns = list(Mean = mean, SD = sd),  
    .names = "{col}_{fn}"))



#DESIGN FUNCTION FOR CALCULATING T-STATS ON LAMBDAS AND FIRST ORDER AUTO-CORRELATION OF LAMBDAS
#REPEAT STATISTICS ESTIMATION AS DONE ABOVE
tstat <- function(x, na.rm = FALSE) {
  if(na.rm){ #if na.rm is TRUE, remove NA values from input x
    x = x[!is.na(x)]
  }
  mean<- mean(x)
  sd <- sd(x)
  tstat<-mean(x)/(sd(x)/sqrt(n()))
}



corr <- function(x, na.rm = FALSE) {
  if(na.rm){ #if na.rm is TRUE, remove NA values from input x
    x = x[!is.na(x)]
  }
  mean<- 0
  cor(x-mean, lag(x)-mean, use = "na.or.complete")
}



#OBTAIN T-STATISTICS
#FULL WINDOW
summary2A<-lambdas_with_rf |>
  summarise(across(
    .cols = c(lambda0, lambda1, lambda2, lambda3), 
    .fns = list(tstat=tstat),  
    .names = "{col}_{fn}"))



#LONG RANGE WINDOWS
summary2B<-lambdas_with_rf |>
  group_by(out)|>
  summarise(across(
    .cols = c(lambda0, lambda1, lambda2, lambda3), 
    .fns = list(tstat=tstat),  
    .names = "{col}_{fn}"))



#SHORTER RANGE WINDOWS 
summary2C<-lambdas_with_rf |>
  group_by(elt)|>
  summarise(across(
    .cols = c(lambda0, lambda1, lambda2, lambda3), 
    .fns = list(tstat=tstat),  
    .names = "{col}_{fn}"))



#OBTAIN FIRST ORDER AUTOCORRELATION
summary3A<-lambdas_with_rf |>
  summarise(across(
    .cols = c(lambda1, lambda2, lambda3), 
    .fns = list(corr=corr),  
    .names = "{col}_{fn}"))





###############################################

