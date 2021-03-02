#! /usr/bin/env python3

import pandas as pd
import os, datetime
from config import *

emp_file = 'emp.csv'
locs = ["Dimond", "Uptown"]

# To use this func, df should have datetime convertible index
# Should be all numbers to use this program
def dfWithDateIndexNoDollar(df):
    df.index = pd.to_datetime(df.index)
    df = df.replace( '[\$,)]', '', regex=True ). \
        replace( '[(]', '-', regex=True)
    df = df.apply(pd.to_numeric, downcast='float', errors='ignore')
    return df

# Functions for agg 
def comm25(x):
    sum = 0
    for i in x:
        sum += i
    return sum * 0.25

def comm50(x):
    sum = 0
    for i in x:
        sum += i
    return sum * 0.5

# For night time report writing
def forNight(raw_df, grp_df):
	forNight_df = pd.DataFrame(index = grp_df.index, \
		columns = ['Night_Total', 'Date', 'Memo'])
	pDate = raw_df.index[-1] + datetime.timedelta(5)
	sDate = raw_df.index[0]
	eDate = raw_df.index[-1]	
	for pname in grp_df.index:
		forNight_df['Night_Total'][pname] = grp_df['Total'][pname]
		forNight_df['Date'][pname] = pDate
		forNight_df['Memo'][pname] = f'Night commission for {sDate}-{eDate}'
	return forNight_df

# Bring Night DataFrames from shared/Umami and read the Night files
dfs = []
for Num, loc in enumerate(locs):
    try:
        fname = os.path.join(path_shared, period, report,\
            period + '_' + loc + '_Night.csv')      
        df = pd.read_csv(fname, index_col=[0], header=[0])
    except:
        print(f'----file related to {loc} is not present----')
        continue
        
    # df.index = pd.to_datetime(df.index) # Need to be set before dfWithDateIndexNoDollar function call
    df = dfWithDateIndexNoDollar(df)
    df = df.replace(to_replace = ['P', 'L'], \
        value = ['Sungsoon Park', 'Sang Jin Lee']) #.dropna(how='any')
    df.index = df.index.date
    dfs.append(df)

# Calculation
for n in range(len(dfs)):
    dfw = dfs[n]
    dfgrp = dfw.groupby('Name').agg(
        {
            'Gross Sales': comm50,
            'Tip': 'sum',
            'doordash': comm25,
            'uber': comm25
        }
    )
    dfgrp['Total'] = round(dfgrp.apply(sum, axis=1), 2)
    # Make report for saving
    night_check_print = forNight(dfw, dfgrp)

    filename = os.path.join(path_shared, period, report, period + '_' + locs[n] + '_Night_Claculated.xlsx')
    with pd.ExcelWriter(filename) as writer:
        dfw.to_excel(writer, sheet_name=locs[n]+'_'+'Night')
        dfgrp.to_excel(writer, sheet_name=locs[n]+'_calculates')
        night_check_print.to_excel(writer, sheet_name=locs[n]+'_For_Check_Print')

