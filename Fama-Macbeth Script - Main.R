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

setwd("C:/Fama_macbeth/Fama-Macbeth-Group-Work")
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

#SECTION G - CONSTRUCT TABLE 4
#1. need to calculate port returns' mean and sd 
#sAVE IT AS TIBBLE

#TABLE 4 SUMMARIES
#SUMMARY 1 - AVERAGE AND STANDARD DEVIATION OF LAMBDAS
lambdas_with_rf$no_grouping <- "1935 to 1968"

summary1<-lambdas_with_rf |>
  group_by("1935 to 1968")|>
  summarise(across(
    .cols = c(lambda0, lambda1, lambda0rf, rsquare), 
    .fns = list(Mean = mean, SD = sd),  
    .names = "{col}_{fn}"))

summary2<-lambdas_with_rf |>
  group_by(out)|>
  summarise(across(
    .cols = c(lambda0, lambda1, lambda0rf, rsquare), 
    .fns = list(Mean = mean, SD = sd),  
    .names = "{col}_{fn}"))

summary3<-lambdas_with_rf |>
  group_by(elt)|>
  summarise(across(
    .cols = c(lambda0, lambda1, lambda0rf, rsquare), 
    .fns = list(Mean = mean, SD = sd),  
    .names = "{col}_{fn}"))


colnames(summary3)[1] <- "Period"
colnames(summary2)[1] <- "Period"
colnames(summary1)[1] <- "Period"

summary_total <- bind_rows(summary1,summary2,summary3)

#1. need to calculate port returns' mean and sd 
risk_free<-ff_3factors_mon|>
  select(rf, year_month)

stacked_data <- read.csv("stacked_data_cross_sectional_regression.csv")
stacked_data <- as_tibble(stacked_data)

stacked_data$no_grouping <- "1935 to 1968"
stacked_data$date <- as.Date(stacked_data$date, format = "%d/%m/%Y")
stacked_data$year_month <- format(stacked_data$date, "%Y-%m")

stacked_data <- stacked_data |> left_join(risk_free, by = "year_month")|>
  mutate(rf=rf/100)
stacked_data$rm_minus_rf <- stacked_data$mkt_Port_Mean - stacked_data$rf

names(stacked_data)

table4_test1 <- stacked_data |> group_by("1935 to 1968") |> 
  summarise(mkt_ret = mean(mkt_Port_Mean),rm_minus_rf = mean(rm_minus_rf) ,rf_mean = mean(rf), market_sharpe_ratio = mean(rm_minus_rf)/sd(mkt_Port_Mean), mkt_ret_sd = sd(mkt_Port_Mean),rf_sd = sd(rf))
table4_test2 <- stacked_data |> group_by(subperiodlarge) |> 
  summarise(mkt_ret = mean(mkt_Port_Mean),rm_minus_rf = mean(rm_minus_rf) ,rf_mean = mean(rf), market_sharpe_ratio = mean(rm_minus_rf)/sd(mkt_Port_Mean), mkt_ret_sd = sd(mkt_Port_Mean),rf_sd = sd(rf))
table4_test3 <- stacked_data |> group_by(subperiodsmall) |> 
  summarise(mkt_ret = mean(mkt_Port_Mean),rm_minus_rf = mean(rm_minus_rf) ,rf_mean = mean(rf), market_sharpe_ratio = mean(rm_minus_rf)/sd(mkt_Port_Mean), mkt_ret_sd = sd(mkt_Port_Mean),rf_sd = sd(rf))

colnames(table4_test1)[1] <- "Period"
colnames(table4_test2)[1] <- "Period"
colnames(table4_test3)[1] <- "Period"


summary_rm_and_rf <- bind_rows(table4_test1,table4_test2,table4_test3)
summary_rm_and_rf

summary_total
summary_rm_and_rf
names(summary_total)
names(summary_rm_and_rf)

Table4_summary <- left_join(summary_total, summary_rm_and_rf, by = "Period")
Table4_summary$lambda1_Mean_by_mkt_ret_sd <- Table4_summary$lambda1_Mean/Table4_summary$mkt_ret_sd

Table4_summary1 <- Table4_summary|> select(Period, mkt_ret, rm_minus_rf, lambda1_Mean, lambda0_Mean,
                                           rf_mean, market_sharpe_ratio, lambda1_Mean_by_mkt_ret_sd, mkt_ret_sd, 
                                           lambda1_SD, lambda0_SD, rf_sd)
Table4_summary1

#DESIGN FUNCTION FOR CALCULATING T-STATS ON LAMBDAS AND FIRST ORDER AUTO-CORRELATION OF LAMBDAS
lambdas_with_rf_new <- stacked_data |>  filter(port == 1) |> left_join(lambdas_with_rf, by = c("year_month","date", "rf", "no_grouping")) |>
  select(date, out, elt, lambda0, lambda1, rsquare, year_month, rf, lambda0rf, no_grouping, mkt_Port_Mean, rm_minus_rf, )
lambdas_with_rf_new$obs <- 1
lambdas_with_rf_new |> mutate(mkt_ret_tstat = (mean(mkt_Port_Mean)*sqrt(sum(obs)))/(sd(mkt_Port_Mean)))
names(lambdas_with_rf_new)
lambdas_with_rf_new$no_grouping

for_tstat1 <- lambdas_with_rf_new |> group_by(no_grouping) |> 
  summarise(mkt_ret_tstat = (mean(mkt_Port_Mean)*sqrt(sum(obs)))/(sd(mkt_Port_Mean)), 
            rm_minus_rf_tstat = (mean(rm_minus_rf)*sqrt(sum(obs)))/(sd(rm_minus_rf)),
            lambda1_tstat = (mean(lambda1)*sqrt(sum(obs)))/(sd(lambda1)), 
            lambda0_tstat = (mean(lambda0)*sqrt(sum(obs)))/(sd(lambda0)),
            corr_mkt_ret = cor(mkt_Port_Mean,lag(mkt_Port_Mean), use = "na.or.complete"),
            corr_rm_minu_rf = cor(rm_minus_rf,lag(rm_minus_rf), use = "na.or.complete"),
            corr_lambda1 = cor(lambda1,lag(lambda1), use = "na.or.complete"),
            corr_lambda0 = cor(lambda0,lag(lambda0), use = "na.or.complete"),
            corr_rf = cor(rf,lag(rf), use = "na.or.complete"))

for_tstat2 <- lambdas_with_rf_new |> group_by(out) |> 
  summarise(mkt_ret_tstat = (mean(mkt_Port_Mean)*sqrt(sum(obs)))/(sd(mkt_Port_Mean)), 
            rm_minus_rf_tstat = (mean(rm_minus_rf)*sqrt(sum(obs)))/(sd(rm_minus_rf)),
            lambda1_tstat = (mean(lambda1)*sqrt(sum(obs)))/(sd(lambda1)), 
            lambda0_tstat = (mean(lambda0)*sqrt(sum(obs)))/(sd(lambda0)),
            corr_mkt_ret = cor(mkt_Port_Mean,lag(mkt_Port_Mean), use = "na.or.complete"),
            corr_rm_minu_rf = cor(rm_minus_rf,lag(rm_minus_rf), use = "na.or.complete"),
            corr_lambda1 = cor(lambda1,lag(lambda1), use = "na.or.complete"),
            corr_lambda0 = cor(lambda0,lag(lambda0), use = "na.or.complete"),
            corr_rf = cor(rf,lag(rf), use = "na.or.complete"))

for_tstat3 <- lambdas_with_rf_new |> group_by(elt) |> 
  summarise(mkt_ret_tstat = (mean(mkt_Port_Mean)*sqrt(sum(obs)))/(sd(mkt_Port_Mean)), 
            rm_minus_rf_tstat = (mean(rm_minus_rf)*sqrt(sum(obs)))/(sd(rm_minus_rf)),
            lambda1_tstat = (mean(lambda1)*sqrt(sum(obs)))/(sd(lambda1)), 
            lambda0_tstat = (mean(lambda0)*sqrt(sum(obs)))/(sd(lambda0)),
            corr_mkt_ret = cor(mkt_Port_Mean,lag(mkt_Port_Mean), use = "na.or.complete"),
            corr_rm_minu_rf = cor(rm_minus_rf,lag(rm_minus_rf), use = "na.or.complete"),
            corr_lambda1 = cor(lambda1,lag(lambda1), use = "na.or.complete"),
            corr_lambda0 = cor(lambda0,lag(lambda0), use = "na.or.complete"),
            corr_rf = cor(rf,lag(rf), use = "na.or.complete"))

colnames(for_tstat1)[1] <- "Period"
colnames(for_tstat2)[1] <- "Period"
colnames(for_tstat3)[1] <- "Period"

tstat_summary <- bind_rows(for_tstat1,for_tstat2,for_tstat3)

names(summary_total)
names(Table4_summary1)
names(tstat_summary)

Table4_summary1 <- Table4_summary1 |> select(Period, mkt_ret, rm_minus_rf, lambda1_Mean, lambda0_Mean, rf_mean,
                                             market_sharpe_ratio, lambda1_Mean_by_mkt_ret_sd, mkt_ret_sd, lambda1_SD, lambda0_SD, rf_sd)
tstat_summary <- tstat_summary |> select(Period, mkt_ret_tstat, rm_minus_rf_tstat, lambda1_tstat, lambda0_tstat, 
                                         corr_mkt_ret, corr_rm_minu_rf, corr_lambda1, corr_lambda0, corr_rf)
Table4_total <- left_join(Table4_summary1, tstat_summary, by ="Period")
Table4_total



####################################
#APPENDIX - SINGLE LINE FORMULAS OF MEASURING SERIAL CORRELATION
lambdas_with_rf$lambda1
cor(lambdas_with_rf$lambda1-mean(lambdas_with_rf$lambda1), lag(lambdas_with_rf$lambda1)-mean(lambdas_with_rf$lambda1), use = "na.or.complete")
cor(lambdas_with_rf$lambda2, lag(lambdas_with_rf$lambda2), use = "na.or.complete")
cor(lambdas_with_rf$lambda3, lag(lambdas_with_rf$lambda3), use = "na.or.complete")
lambdas_with_rf_1935_1945 <- lambdas_with_rf |> filter(out == "1935 to 1945")


cor(lambdas_with_rf_1935_1945$lambda1-mean(lambdas_with_rf_1935_1945$lambda1), lag(lambdas_with_rf_1935_1945$lambda1)-mean(lambdas_with_rf_1935_1945$lambda1), use = "na.or.complete")
cor(lambdas_with_rf_1935_1945$lambda2, lag(lambdas_with_rf_1935_1945$lambda2), use = "na.or.complete")
cor(lambdas_with_rf_1935_1945$lambda3, lag(lambdas_with_rf_1935_1945$lambda3), use = "na.or.complete")



###################################
