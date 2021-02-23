#! /usr/bin/env python3


'''
This program is for converting 7shift report to excel 
format table and output is saved under report.

***Prerequisite***
emp.csv
7shift report file = 2020_05A.csv format
output saved under report
'''

import pandas as pd
import os, sys, datetime, openpyxl
# config file path append to call config.py
# sys.path.append(os.path.abspath('/media/sf_shared/my_program/scripts/Finance_Umami'))
from config import *

shift_file = period + biMonth + '.csv'
emp_file = 'emp.csv'

# To use this func, df should have datetime convertible index
# Work on df to make index datetime, and convert currency to float
def dfWithDateIndexNoDollar(df):
    df.index = pd.to_datetime(df.index)
    df = df.replace( '[\$,)]', '', regex=True ). \
        replace( '[(]', '-', regex=True)
    df = df.apply(pd.to_numeric, downcast='float', errors='ignore')
    return df



# Data/emp.csv to df; punch ID dropna & float --> str
df = pd.read_csv(os.path.join(path, data, emp_file), header = [0])
df.dropna(subset = ["Punch ID"], inplace = True)
df.drop(columns=["Locations", "Departments", "Roles"], inplace = True)
df["Punch ID"] = df["Punch ID"].astype('int').astype('str') # Convert Int before str to remove extra .0
# df["Punch ID"] = df["Punch ID"].astype('str')

# Make emp dict: Punch ID: ["First Name last name", "wage"]
emp = {}
for ne, id in enumerate(df["Punch ID"]):
    emp[id] = [df.iloc[ne, 0] + ' ' + df.iloc[ne, 1], df.iloc[ne, 4]]

# set header to 5th row
df = pd.read_csv(os.path.join(path_shared, period, meta, shift,\
    shift_file), header=[5])

# errors = 'coerce' makes str NAN; downcast='integer' should be int but not this case
df['Employee ID'] = pd.to_numeric(df['Employee ID'], errors='coerce')
# dropna()
df.dropna(subset=['Employee ID'], inplace = True)
# make int again and str
df['Employee ID'] = df['Employee ID'].astype('int').astype('str')

# to_datetime
currentyear = datetime.datetime.now().year
df['Date'] = df['Date'].apply(lambda x: x + ', ' + str(currentyear))
df['Date'] = pd.to_datetime(df['Date'])
# Add column for day of the week
df['Day'] = df['Date'].dt.day_name() # if index, no dt. df.index.day_name()

# shift time splits using .split
# df['start_time'], df['end_time'] = df['Shift Details'].str.split(' - ', n = 1, expand = True)

# Better way
df['start_time'], df['end_time'] = zip(*df['Shift Details'].str.split(' - '))

# sort historically
df.rename(columns={'Regular':'Hours'}, inplace=True)
df.sort_values(by = ['Date'], inplace = True)
df = df[['Date', 'Day', 'Employee ID', 'Hours', 'start_time', 'end_time', 'Location']]

dim = (df['Location'] == 'Dimond')
upt = (df['Location'] == 'Uptown')

df_Dim = df[dim].drop(columns=['Location'])
df_Upt = df[upt].drop(columns=['Location'])

umami = [df_Dim, df_Upt]
locs = ['Dimond', 'Uptown']
for k, d in enumerate(umami):
    # index reset to replace item; drop = True not to new column for old indx
    d.reset_index(drop=True, inplace = True)
    # Change ID to name
    for num, eid in enumerate(d['Employee ID']):
        d.iloc[num, 2] = emp[eid][0]
    # Change Employee ID to name
    d.rename(columns={'Employee ID': 'Name'}, inplace = True)
    # Hours to float, pivot need some number to crunch!
    d['Hours'] = d['Hours'].astype('float')
    # df_Dim.dtypes: aggfunc default is 'mean', so it should be 'sum'
    dpivot = pd.pivot_table(d,
        index = ['Date', 'Day'], 
        columns = ['Name'],
        aggfunc = 'sum', 
        values = 'Hours')
    
    fname = os.path.join(path_shared, period, report, period + biMonth +'_'+ locs[k] + '_Work_Hrs' + '.csv')
    if not os.path.isfile(fname):
        with open(fname, 'w') as f:
            dpivot.to_csv(f, header=True)

    # paths = [path_shared, path]
    # for pth in paths:
    #     filename = os.path.join(pth, period, report, period + biMonth +'_'+ locs[k] + '_Work_Hrs' + '.csv')
    #     with open(filename, 'w') as f:
    #         dpivot.to_csv(f, header=True)


# Making Dummie excel table for manual input and cash income calculation
# Columns will be Dimond_Day, Dimond_Night, Uptown_Day, Uptown_Night etc
locs = ['Dimond', 'Uptown']
manual_input_df = pd.DataFrame()
for loc in locs:
    tod = ['Day', 'Night']
    for td in tod:
        try:
            cashfile = os.path.join(
                path_shared, period, meta, square, \
                    'pmethod_'+biMonth+'_'+loc+ '_'+td+ '.csv'
            )
            cash_df = pd.read_csv(cashfile, index_col = [0], header = [0])
            cash_df = dfWithDateIndexNoDollar(cash_df.T)
            cash_df = cash_df['Cash'].rename(loc+'_'+td)
        # Report in case of no transaction have 'Unknown Error' columns. Need to drop that
        except:
            print(f'File related to {td} cash for {loc} not exsit! Replacing with Dummie...')
            cash_df = cash_df.drop(columns=['Unknown Error'])
            cash_df[loc+'_'+td] = 0.0
            pass

        manual_input_df = pd.concat([manual_input_df, cash_df], axis = 1)

manual_input_df.index = pd.to_datetime(manual_input_df.index)
manual_input_df["Day"] = manual_input_df.index.day_name()
manual_input_df.index = pd.to_datetime(manual_input_df.index).date

for loc in locs:
    manual_input_df['initials worked at_'+loc+'_Night'] = ""

# Make cash counting page 
cash_dummie = pd.DataFrame(columns = ['bills', 'Dimond_ct', 'Dimond_cash', 'Uptown_ct', 'Uptown_cash'])
cash_dummie['bills'] = ['100', '50', '20', '10', '5', '1', 'Total', 'Adj_Total', ' ']
cash_dummie = cash_dummie.set_index('bills')



# Make a file for manual input. Append to previous date
filename = os.path.join(path_shared, period, report, period + '_' + 'manual_input.xlsx')
if os.path.isfile(filename):
    sheet1 = pd.read_excel(filename, sheet_name='Cash_Payment', index_col=0, header=0)
    sheet1.index = pd.to_datetime(sheet1.index).date
    sheet2 = pd.read_excel(filename, sheet_name='For_Cash_Calc', index_col=0, header=0)

    manual_input_df = pd.concat([sheet1, manual_input_df], axis=0)
    cash_dummie = pd.concat([sheet2, cash_dummie], axis=0)

with pd.ExcelWriter(filename) as writer:
    manual_input_df.to_excel(writer, sheet_name='Cash_Payment')
    cash_dummie.to_excel(writer, sheet_name='For_Cash_Calc')

