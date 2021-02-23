#! /usr/bin/env python3

import pandas as pd
import os, sys
from datetime import datetime
# config file path append to call config.py
sys.path.append(os.path.abspath('/media/sf_shared/my_program/scripts/Finance_Umami'))
# sys.path.append(os.path.abspath('/home/sung/Umami/scripts/Finance_Umami'))
from config import *

# Function which sort_index by ascending; only save the month of May; write week_of_day   
def refine(df):
    df.sort_index(ascending=True, inplace = True)
    df = df.loc[(df.index.year == year) & (df.index.month == month)]
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

# Uber data from meta/uber
locs = ["Dimond", "Uptown"]
for nu, udf in enumerate(locs):
    udf = os.path.join(path_shared, period, meta, uber, \
        'Uber_'+period+'_'+ locs[nu]+'.csv')
    globals()['udf'+locs[nu]] = pd.read_csv(udf, header = [0])

# Dimond and Uptown uber revenue
locs = ["Dimond", "Uptown"]
udfs = [udfDimond, udfUptown]

umami = pd.DataFrame()


for n, dfu in enumerate(udfs):
    udfs[n] = dfu.loc[:, ['Order Date / Refund date', 'Order Accept Time', 'Food Sales (excluding tax)', 'Tax on Food Sales', 'Uber Service Fee', 'Payout']]
    # To remove NAN in Date columns, otherwise error
    udfs[n].dropna(subset=['Order Date / Refund date'], inplace=True)
    # combine date and time, assign datetime obj
    udfs[n]['Date'] = udfs[n][['Order Date / Refund date',\
        'Order Accept Time']].apply(' '.join, axis=1).apply(pd.to_datetime)
    udfs[n].set_index('Date', inplace=True)
    udfs[n].drop(columns = ['Order Date / Refund date', 'Order Accept Time'], inplace = True)
    udfs[n].columns = ['Price', 'Tax', 'Fee', 'Umami portion']
    udfs[n]['Location'] = locs[n]
    umami = umami.append(udfs[n])


# for n, dfu in enumerate(udfs):
#     udfs[n] = dfu[
#         ['Order Date / Refund date', 
# 'Order Accept Time', 
# 'Food Sales (excluding tax)', 
# 'Tax on Food Sales', 
# 'Uber Service Fee', 
# 'Payout']
#     ]
#     # To remove NAN in Date columns, otherwise error
#     udfs[n].dropna(subset=['Order Date / Refund date'], inplace=True)
#     # combine date and time, assign datetime obj
#     udfs[n]['Date'] = udfs[n][['Order Date / Refund date',\
#         'Order Accept Time']].apply(' '.join, axis=1).apply(pd.to_datetime)
#     udfs[n].set_index('Date', inplace=True)
#     udfs[n].drop(columns = ['Order Date / Refund date', 'Order Accept Time'], inplace = True)
#     udfs[n].columns = ['Price', 'Tax', 'Fee', 'Umami portion']
#     udfs[n]['Location'] = locs[n]
#     umami = umami.append(udfs[n])

grouped = umami.groupby('Location')
grp_calc = grouped.sum()

fname_uber_calc = os.path.join(path_shared,period, report, period+'_'+'Umami_uber_calculated.csv')
with open(fname_uber_calc, 'w') as f:
    grp_calc.to_csv(f, header=True)

# paths = [path_shared, path]
# for pth in paths:
#     fname_uber_calc = os.path.join(pth,period, report, period+'_'+'Umami_uber_calculated.csv')
#     with open(fname_uber_calc, 'w') as f:
#         grp_calc.to_csv(f, header=True)

# Umami Total Uber sales save to csv file under report
umamiAll = refine(umami)

fname_umami = os.path.join(path_shared,period, report, period+'_'+'Umami_uber.csv')
with open(fname_umami, 'w') as f:
    umamiAll.to_csv(f, header = True)

# paths = [path_shared, path]
# for pth in paths:
#     fname_umami = os.path.join(pth,period, report, period+'_'+'Umami_uber.csv')
#     with open(fname_umami, 'w') as f:
#         umamiAll.to_csv(f, header = True)

# For night data; pd object need to be saved in different name object
dfNight = umamiAll.between_time('15:30', '22:00')

# groupby 'Location', total caviar data for sales tax calculation
grp_night = dfNight.groupby('Location')
# split Dimond and Uptown night income
# groupby obj.groups works like dict.keys, but no ()
# get_group(groups) serves like dict.get(keys)
grp_list = [grp_night.get_group(x) for x in grp_night.groups]

# Dimond is grp_list[0] and Uptown is grp_list[1], no income yet!!
locs = ["Dimond", "Uptown"]
for u, df in enumerate(grp_list):
    fnmae_night = os.path.join(path_shared,period, report, period + '_uber' + '_' + locs[u] + '_Night.csv')
    with open(fnmae_night, 'w') as f:
        df.to_csv(f, header=True)

    # paths = [path_shared, path]
    # for pth in paths:
    #     fnmae_night = os.path.join(pth,period, report, period + '_uber' + '_' + locs[u] + '_Night.csv')
    #     with open(fnmae_night, 'w') as f:
    #         df.to_csv(f, header=True)


