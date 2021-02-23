#! /usr/bin/env python3

import pandas as pd
import sys, os, datetime, re
# config file path append to call config.py
sys.path.append(os.path.abspath('/media/sf_shared/my_program/scripts/Finance_Umami'))
# sys.path.append(os.path.abspath('/home/sung/Umami/scripts/Finance_Umami'))
from config import *

# To use this func, df should have datetime convertible index
# Should be all numbers to use this program
def dfWithDateIndexNoDollar(df):
    df.index = pd.to_datetime(df.index)
    df = df.replace( '[\$,)]', '', regex=True ). \
        replace( '[(]', '-', regex=True)
    df = df.apply(pd.to_numeric, downcast='float', errors='ignore')
    return df

locs = ['Dimond', 'Uptown']
for n, loc in enumerate(locs):
	square_night_file = period + 'N_' + locs[n] + '.csv'
	# caviar_night_file = period + '_caviar_' + locs[n] + '_Night.csv'
	uber_night_file = period + '_uber_' + locs[n] + '_Night.csv'
	night_worker_file = period + '_manual_input.xlsx'

	folder = os.path.join(path_shared, period)
	try:
		dfsq = pd.read_csv(folder+'/meta/square/'+square_night_file, index_col=[0], header=[0]).T
		# dfcv = pd.read_csv(folder+'/report/'+caviar_night_file, index_col=[0], header=[0])
		dfub = pd.read_csv(folder+'/report/'+uber_night_file, index_col=[0], header=[0])
		dfnw = pd.read_excel(folder+'/report/'+night_worker_file, sheet_name='Cash_Payment', index_col=0, header=0)
		dfnw.index = pd.to_datetime(dfnw.index).date

	except FileNotFoundError as e:
		print(f'File related to {locs[n]} not present. \n\
		Processing has been sucessfully done upto {locs[n-1]} data')
		continue

	dfsq = dfWithDateIndexNoDollar(dfsq).drop(columns= ['Returns', 'Discounts & Comps', \
		'Net Sales', 'Gift Card Sales', 'Refunds by Amount', \
		'Payments', 'Total Collected', 'Fees', 'Net Total', 'Tax', 'Total'])

	# dfcv = dfWithDateIndexNoDollar(dfcv).drop(columns=['Tax', 'Fee', 'Umami portion'])
	dfub = dfWithDateIndexNoDollar(dfub).drop(columns=['Tax', 'Fee', 'Umami portion'])
	dfnw = dfnw[['initials worked at_Dimond_Night']]

	# Make a copy of umami df
	globals()[locs[n] + '_cu'] = dfsq.dropna(how='all', axis=1)
		
	deliveries = [dfub]  # dfcv
	for nd, df in enumerate(deliveries):
		# df.reset_index(inplace=True)
		# df = df.groupby([pd.Grouper(key='Pickup time', freq='d'), 'day', 'Location'])['Price'].sum()
		# dfcv = dfcv.reset_index().set_index('Pickup time')
		df = df.resample('d').sum()
		globals()[locs[n] + '_cu'] = pd.concat([globals()[locs[n] + '_cu'], df], axis=1).fillna(0.0)
	
	globals()[locs[n] + '_cu']['Day'] = globals()[locs[n] + '_cu'].index.day_name()
	globals()[locs[n] + '_cu'] = pd.concat([globals()[locs[n] + '_cu'], dfnw], axis=1)
	globals()[locs[n] + '_cu'].columns = ['Gross Sales', 'Tip', 'uber', 'Day', 'Name'] # 'caviar'
	# globals()[locs[n] + '_cu']['Name'] = "" # Make Empty columns for input later
	filename = os.path.join(path_shared, period, report, period +'_'+ locs[n] + '_Night' + '.csv')
	with open(filename, 'w') as f:
		globals()[locs[n]+'_cu'].to_csv(f, header=True)

	# paths = [path_shared, path]
	# for pth in paths:
	# 	filename = os.path.join(pth, period, report, period +'_'+ locs[n] + '_Night' + '.csv')
	# 	with open(filename, 'w') as f:
	# 		globals()[locs[n]+'_cu'].to_csv(f, header=True)

	print(f'{filename} has been sucessfully inplemented')

