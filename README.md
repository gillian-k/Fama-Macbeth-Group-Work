# Fama-Macbeth-Group-Work [Gillian Kimundi, Dongho Kim and Ming Fan]

The relationship between risk and return in financial markets sets the tone for the two-parameter portfolio model. The testable implications of the model are that firstly, expected returns are linearly related to risk; second, the beta is a complete risk measure for any security and thirdly, market risk should bear a positive premium, such that higher risk is associated with a higher return.

In this project, we attempt to replicate the findings in Fama & Macbeth's (1973) seminal paper. 

We begin by subsetting the stock data into similar periods as used in the original paper, where subperiods consist of portfolio formation data (for estimation of pre-ranking betas), a main estimation window (for estimation of portfolio betas) and a testing period (for the cross sectional regression of portfolio betas against portfolio returns)

#For the cross sectional regression, three variables are required, and these are contained in the CSV file labelled "stacked_data_cross_sectional_regression.csv"

##---->the portfolio beta (average of stock betas) to capture market risk
##---->the square portfolio beta, initially measured at stock level, to capture non-linear market risk
##---->the portfolio standard deviation of residuals, initially measured at stock level, to capture non-beta market risk

The CSV dataset is structured such that the returns on the stock are merged with the estimated betas, residuals, and sd(residuals) from the previous year, hence the column called "beta_year". This is necessary since the cross sectional regression relies on lagged independent variables. 


