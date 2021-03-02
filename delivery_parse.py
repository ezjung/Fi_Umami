#! /usr/bin/env python3

import pandas as pd
import os, sys
from datetime import datetime
# config file path append to call config.py
# sys.path.append(os.path.abspath('/media/sf_shared/my_program/scripts/Finance_Umami'))
# sys.path.append(os.path.abspath('/home/sung/Umami/scripts/Finance_Umami'))
from config import *

delivery_list = ['Uber', 'Doordash']

# Function which sort_index by ascending; only save the month of May; write week_of_day   
def refine(df):
    df.sort_index(ascending=True, inplace = True)
    df = df.loc[df.index.year == year]
    df = df.loc[df.index.month == month]
    # df = df.loc[(df.index.year == year) & (df.index.month == month)] # creates SettingWithCopyWarning
    df['day'] = df.index.day_name()
    return df

# To use this func, df should have datetime convertible index
# Should be all numbers to use this program
def dfWithDateIndexNoDollar(df):
    df.index = pd.to_datetime(df.index)
    df = df.replace( '[\$,)]', '', regex=True ). \
        replace( '[(]', '-', regex=True)
    df = df.apply(pd.to_numeric, downcast='float', errors='ignore')
    return df

# Combineing uber [dim and upt] and doordash [dim and upt]
# Should be alphabetical order
locs = ["Dimond", "Uptown"]


# Uber df which will be appended with dimond and uptown both
ub_df = pd.DataFrame()
for location in locs:
    uberfile = os.path.join(path_shared, period, meta, uber, \
        'Uber_'+period+'_'+ location +'.csv')
    udf = pd.read_csv(uberfile, header = [0])
    ub_df = ub_df.append(udf, ignore_index=True)


# Doordash df which will be appended with dimond and uptown both
dd_df = pd.DataFrame()
for location in locs:
    doordashfile = os.path.join(path_shared, period, meta, doordash, \
        'Doordash_'+period+'_'+ location +'.csv')
    ddf = pd.read_csv(doordashfile, header = [0])
    dd_df = dd_df.append(ddf, ignore_index=True)



# Uber revenue Sector

ub_df = ub_df.loc[:, 
    ['Store Name', 
    'Order Date / Refund date', 
    'Order Accept Time', 
    'Food Sales (excluding tax)', 
    'Tax on Food Sales', 
    'Uber Service Fee', 
    'Payout']]

ub_df.rename(columns={'Store Name': 'Location'}, inplace = True)
ub_df.replace({'Cafe Umami':'Dimond', 'Cafe Umami Uptown':'Uptown'}, inplace=True)
ub_df.dropna(subset=['Order Date / Refund date'], inplace=True)

ub_df['Date'] = ub_df[['Order Date / Refund date', 'Order Accept Time']].\
    apply(' '.join, axis=1).apply(pd.to_datetime)

ub_df.set_index('Date', inplace=True)
ub_df.drop(columns = ['Order Date / Refund date', 'Order Accept Time'], inplace = True)
ub_df.columns = ['Location', 'Price', 'Tax', 'Fee', 'Umami portion']
# ub_df['Fee'] = ub_df['Fee'].apply(lambda x: x * -1)

# Doordash revenue sector
dd_df = dd_df.loc[:, 
    ['STORE_ID', 
    'TIMESTAMP_LOCAL_DATE', 
    'TIMESTAMP_LOCAL_TIME', 
    'SUBTOTAL', 
    'TAX_SUBTOTAL', 
    'COMMISSION', 
    'CREDIT']]

dd_df = dd_df[dd_df['CREDIT'] > 0]
dd_df.rename(columns={'Store Name': 'Location'}, inplace = True)
dd_df.replace({177834:'Dimond', 685471:'Uptown'}, inplace=True)
dd_df.dropna(subset=['TIMESTAMP_LOCAL_DATE'], inplace=True)
dd_df['Date'] = dd_df[['TIMESTAMP_LOCAL_DATE', 'TIMESTAMP_LOCAL_TIME']].\
    apply(' '.join, axis=1).apply(pd.to_datetime)
dd_df.set_index('Date', inplace=True)
dd_df.drop(columns = ['TIMESTAMP_LOCAL_DATE', 'TIMESTAMP_LOCAL_TIME'], inplace = True)
dd_df.columns = ['Location', 'Price', 'Tax', 'Fee', 'Umami portion']


# Total summary of delivery services
ub_df_sum = ub_df.groupby('Location').sum()
dd_df_sum = dd_df.groupby('Location').sum()

df_sum_list = [ub_df_sum, dd_df_sum]

for n, i in enumerate(df_sum_list):
    fname = os.path.join(path_shared,period, report, period+'_'+ delivery_list[n] +'_sum.csv')
    with open(fname, 'w') as f:
        i.to_csv(f, header=True)


# Detailed transactions of delivery services Sorted by dates.

ub_df = refine(ub_df)
dd_df = refine(dd_df)

df_list = [ub_df, dd_df]

for n, i in enumerate(df_list):
    fname = os.path.join(path_shared,period, report, period+'_'+ delivery_list[n] +'_detailed_transactions.csv')
    with open(fname, 'w') as f:
        i.to_csv(f, header=True)

# For night data; pd object need to be saved in different name object
ub_dfN = ub_df.between_time('15:30', '22:00')
dd_dfN = dd_df.between_time('15:30', '22:00')

# groupby 'Location', total caviar data for sales tax calculation
ub_grp_night = ub_dfN.groupby('Location')
dd_grp_night = dd_dfN.groupby('Location')
# split Dimond and Uptown night income
# groupby obj.groups works like dict.keys, but no ()
# get_group(groups) serves like dict.get(keys)
ub_grp_list = [ub_grp_night.get_group(x) for x in ub_grp_night.groups]
dd_grp_list = [dd_grp_night.get_group(x) for x in dd_grp_night.groups]

# Dimond is grp_list[0] and Uptown is grp_list[1], no income yet!!

for u, df in enumerate(ub_grp_list):
    fname = os.path.join(path_shared,period, report, period + '_Uber' + '_' + locs[u] + '_Night.csv')
    with open(fname, 'w') as f:
        df.to_csv(f, header=True)

for u, df in enumerate(dd_grp_list):
    fname = os.path.join(path_shared,period, report, period + '_Doordash' + '_' + locs[u] + '_Night.csv')
    with open(fname, 'w') as f:
        df.to_csv(f, header=True)



