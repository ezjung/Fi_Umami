#! /usr/bin/env python3

import pandas as pd
import sys, os, datetime
# config file path append to call config.py
# sys.path.append(os.path.abspath('/media/sf_shared/my_program/scripts/Finance_Umami'))
from config import *

emp_file = 'emp.csv'

# To use this func, df should have datetime convertible index
# Should be all numbers to use this program
def dfWithDateIndexNoDollar(df):
    df.index = pd.to_datetime(df.index)
    df = df.replace( '[\$,)]', '', regex=True ). \
        replace( '[(]', '-', regex=True)
    df = df.apply(pd.to_numeric, downcast='float', errors='ignore')
    return df

# Data/emp.csv to df; punch ID dropna & float --> str
df = pd.read_csv(os.path.join(path, data, emp_file), header = [0])
df.dropna(subset = ['Punch ID'], inplace = True)
df.drop(columns = ["Locations", "Departments", "Roles"], inplace=True)
df["Punch ID"] = df["Punch ID"].astype('int').astype('str')

# Make emp dict, {"Name": "Wage"}
df["Name"] = df["First Name"] + ' ' + df['Last Name']
empWage = {}
for nindex, fullname in enumerate(df['Name']):
	empWage[fullname] = df.iloc[nindex, 4]

# Get work_hr data, and makes them under Dimond and Uptown variables
locs = ['Dimond', 'Uptown']
for fn in locs:
	#globals()[str] makes global variable name
	globals()[fn] = pd.read_csv(os.path.join( \
		path_shared, period, report, \
		period + biMonth + '_' + fn + '_Work_Hrs' + '.csv'), \
		index_col = [0], header = [0]) \
		.drop(columns=['Day'])
	# Make index datetime obj
	globals()[fn].index = pd.to_datetime(globals()[fn].index)

# Get square report to get tip amounts by day
byLoc = ['Dim_df', 'Upt_df']
for ct, fn in enumerate(byLoc):
	globals()[fn + biMonth] = pd.read_csv( \
		os.path.join(path_shared, period, meta, square, \
			period + biMonth + '_' + locs[ct] + '.csv'), index_col = [0]).T 
	# dropna how all is important, to remove the non-opening day
	globals()[fn + biMonth] = dfWithDateIndexNoDollar(globals()[fn + biMonth]). \
		drop(columns = ['Returns', 'Discounts & Comps', \
		'Net Sales', 'Gift Card Sales', 'Refunds by Amount', \
		'Payments', 'Total Collected', 'Fees', 'Net Total', \
		'Gross Sales', 'Tax', 'Total']).dropna(how='all', axis = 1)

	globals()[fn] = globals()[fn + biMonth].join(globals()[locs[ct]]) # join with work_hr df
	globals()[fn].set_index([globals()[fn].index, 'Tip'], inplace=True)
	globals()[fn].fillna(0.0, inplace=True)


def tipCalc(df):
	df['Total Worked Hours'] = df.apply('sum', axis = 1)
	df = df.reset_index()
	df['Tip for calculation'] = df['Tip']
	df.set_index(['level_0', 'Tip'], inplace=True)

	def f(x):
		if x[-2] != 0:
			factor = x[-1] /x [-2]
		else:
			factor = 0
		return round(factor * x, 2)

	df = df.apply(f, axis=1) 
	df.drop(columns=['Total Worked Hours', 'Tip for calculation'], inplace=True)
	return df


locs = ["Dimond", "Uptown"]
for Num, lc in enumerate(byLoc):
	globals()[lc + biMonth] = tipCalc(globals()[lc])
	filename = os.path.join(\
	path_shared, period, report,\
	f'{period}{biMonth}_{locs[Num]}_Tips_dist.csv')

	with open(filename, 'w') as f:
		globals()[lc + biMonth].to_csv(f, header=True)		


# Make Dict for total working hours by the name
def comp(dF):
	compDict = {}
	for name in dF:
		compDict[name] = dF[name].sum()
	return compDict

# a, b are dfs with same type of data so that could be combined
def getTotal(dim, upt):
	Total = {}
	a, b = [comp(dim), comp(upt)]
	for locn in (a, b):
		for pn in locn.keys():
			if pn in Total.keys():
				Total[pn] += locn[pn]
			else:
				Total[pn] = locn[pn]
	return Total

def toCPA(hr_dfa, hr_dfb, tip_dfa, tip_dfb):
	hrTotal = getTotal(hr_dfa, hr_dfb)
	tipTotal = getTotal(tip_dfa, tip_dfb)
	cpa_df = pd.DataFrame(index = hrTotal.keys(), \
		columns = ['Worked Hours', 'Compensation', 'Tips', 'Hourly Wage'])
	for pname in hrTotal.keys():
		cpa_df['Worked Hours'][pname] = hrTotal[pname]
		cpa_df['Tips'][pname] = tipTotal[pname]
		cpa_df['Hourly Wage'][pname] = empWage[pname]
		cpa_df['Compensation'][pname] = hrTotal[pname] * empWage[pname]
	return cpa_df

def forTips(hr_dfa, hr_dfb, tip_dfa, tip_dfb):
	hrTotal = getTotal(hr_dfa, hr_dfb)
	tipTotal = getTotal(tip_dfa, tip_dfb)
	forTips_df = pd.DataFrame(index = hrTotal.keys(), \
		columns = ['Tips', 'Date', 'Memo'])
	hr_dfa.index = pd.to_datetime(hr_dfa.index).date
	hr_dfb.index = pd.to_datetime(hr_dfb.index).date
	pDate = hr_dfa.index[-1] + datetime.timedelta(5)
	sDate = hr_dfa.index[0]
	eDate = hr_dfa.index[-1]	
	for pname in hrTotal.keys():
		forTips_df['Tips'][pname] = tipTotal[pname]
		forTips_df['Date'][pname] = pDate
		forTips_df['Memo'][pname] = f'Tips for {sDate}-{eDate}'
	return forTips_df

forCPA = toCPA(
	globals()[locs[0]], globals()[locs[1]], \
	globals()[byLoc[0] + biMonth], globals()[byLoc[1] + biMonth])

forTipPrint = forTips(
	globals()[locs[0]], globals()[locs[1]], \
	globals()[byLoc[0] + biMonth], globals()[byLoc[1] + biMonth])

filename = os.path.join(path_shared, period, report, period + biMonth + '_' + 'Umami.xlsx')
if not os.path.isfile(filename):
	with pd.ExcelWriter(filename) as writer:
		Dimond.to_excel(writer, sheet_name='Dimond_Hrs')
		Uptown.to_excel(writer, sheet_name='Uptown_Hrs')
		globals()[byLoc[0]+biMonth].to_excel(writer, sheet_name='Dimond_Tips')
		globals()[byLoc[1]+biMonth].to_excel(writer, sheet_name='Uptown_Tips')
		forCPA.to_excel(writer, sheet_name='For_CPA')
		forTipPrint.to_excel(writer, sheet_name='For_TIp_Printing')

else:
	pass