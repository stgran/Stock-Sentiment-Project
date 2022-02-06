from select import select
import pandas as pd

# Import the data from the csv containing the stocks composing the S&P 500
sp500 = pd.read_csv('data_files/sp500_060222.csv')

# Randomly sample 50 of them
selected_stocks = sp500.sample(n = 50)

# Save this sample as a csv
selected_stocks.to_csv('data_files/selected_sp500_sample.csv')