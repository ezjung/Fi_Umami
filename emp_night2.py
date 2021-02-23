#! /usr/bin/env python3

import pandas as pd
import sys, os, datetime
# config file path append to call config.py
sys.path.append(os.path.abspath('/home/sung/Umami/scripts/Finance_Umami'))
from config import *

# shift_file = period + biMonth + '.csv'
emp_file = 'emp.csv'

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

# Data/emp.csv to df; punch ID dropna & float --> str
emp_df = pd.read_csv(os.path.join(path, data, emp_file), header = [0])
emp_df.dropna(subset = ['Punch ID'], inplace = True)
emp_df.drop(columns = ["Locations", "Departments", "Roles"], inplace=True)
emp_df["Punch ID"] = emp_df["Punch ID"].astype('int').astype('str')

# Make emp dict, {"Name": "Wage"}
emp_df["Name"] = emp_df["First Name"] + ' ' + emp_df['Last Name']
empWage = {}
for nindex, fullname in enumerate(emp_df['Name']):
	empWage[fullname] = emp_df.iloc[nindex, 4]

# Bring Night DataFrames from shared/Umami and read the Night files
dfs = []
locs = ["Dimond", "Uptown"]
for Num, loc in enumerate(locs):
    try:
        fname = os.path.join(path_shared, period, report,\
            period + '_' + loc + '_Night.csv')      
        df = pd.read_csv(fname, index_col=[0], header=[0])
    except:
        print(f'----file related to {loc} is not present----')
        continue
        
    df.index = pd.to_datetime(df.index) # Need to be set before dfWithDateIndexNoDollar function call
    df = dfWithDateIndexNoDollar(df)
    # df = df.replace(to_replace = ['P', 'L'], \
    #     value = [emp_df['Name'][12], emp_df['Name'][11]]) #.dropna(how='any')
    df = df.replace(to_replace = ['P', 'L'], \
        value = ['Sungsoon Park', 'Sang Jin Lee']) #.dropna(how='any')
    df.index = df.index.date
    dfs.append(df)
    # # Save to file
    # paths = [path_shared, path]
    # for pth in paths:
    #     filename = os.path.join(pth, period, report, period + '_' + loc + '_Night_Claculated.xlsx')
    #     with pd.ExcelWriter(filename) as writer:
    #         df.to_excel(writer, sheet_name=loc+'_'+'Night')

# Calculation
for nm in range(len(dfs)):
    dfw = dfs[nm]
    dfgrp = dfw.groupby('Name').agg(
        {
            'Gross Sales': comm50,
            'Tip': 'sum',
            # 'caviar': comm25,
            'uber': comm25
        }
    )
    dfgrp['Total'] = round(dfgrp.apply(sum, axis=1), 2)
    # Make report for saving
    night_check_print = forNight(dfw, dfgrp)

    filename = os.path.join(path_shared, period, report, period + '_' + locs[nm] + '_Night_Claculated.xlsx')
    with pd.ExcelWriter(filename) as writer:
        dfw.to_excel(writer, sheet_name=locs[nm]+'_'+'Night')
        dfgrp.to_excel(writer, sheet_name=locs[nm]+'_calculates')
        night_check_print.to_excel(writer, sheet_name=locs[nm]+'_For_Check_Print')


    # Save to file
    # paths = [path_shared, path]
    # for pth in paths:
    #     filename = os.path.join(pth, period, report, period + '_' + locs[nm] + '_Night_Claculated.xlsx')
    #     with pd.ExcelWriter(filename) as writer:
    #         dfw.to_excel(writer, sheet_name=locs[nm]+'_'+'Night')
    #         dfgrp.to_excel(writer, sheet_name=locs[nm]+'_calculates')
    #         night_check_print.to_excel(writer, sheet_name=locs[nm]+'_For_Check_Print')
