import pandas as pd

def extract():
    orders = pd.read_csv('data/raw/List_of_Orders.csv')
    details = pd.read_csv('data/raw/Order Details.csv')
    targets = pd.read_csv('data/raw/Sales target.csv')
    return orders, details, targets
