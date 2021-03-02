#! /usr/bin/env python3

"""
Make new empty directory begining of the month.

Usage:
mkfolder.py 2020 06 A
"""

import sys, os, re

if len(sys.argv) < 4:
    print('''[Program Usage]\n
    mkfolder.py <2020> <08> \n
    Please add argument of year and month in digits, and biMonth in "A" or "B"
    ''')
    quit()

path = '/media/sf_shared/my_program/Financial'
path_shared = '/media/sf_shared/Umami/Financials'


# path = '/home/sung/Umami/Financial'
# path_shared ='/home/sung/shared/Umami/Financials'
path_script = '/media/sf_shared/my_program/scripts/Finance_Umami'
yr, mo, biMonth = str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3])
period = yr + "_" + mo

config_file = os.path.join(path_script, 'config.py')
with open(config_file) as reading_file:

    new_file_content = ""
    regex = re.compile("^yr, mo, biMonth =.*$")
    for line in reading_file:
        new_line = regex.sub(
            f'yr, mo, biMonth = "{sys.argv[1]}", "{sys.argv[2]}", "{sys.argv[3]}"', \
            line, count=1)
        new_file_content += new_line
    with open(config_file, 'w') as f:
        f.write(new_file_content)

meta = 'meta'
report = 'report'

doordash = 'doordash'
uber = 'uber'
shift = 'shift'
square ='square'

directory = os.path.join(path_shared, period)

if os.path.isdir(directory):
    print(f'Directory "{directory}" already exist!')
    quit()

if not os.path.isdir(directory):
    try:
        os.mkdir(directory)
        os.mkdir(os.path.join(path_shared, period, meta))
        os.mkdir(os.path.join(path_shared, period, meta, doordash))
        os.mkdir(os.path.join(path_shared, period, meta, uber))
        os.mkdir(os.path.join(path_shared, period, meta, shift))
        os.mkdir(os.path.join(path_shared, period, meta, square))
        os.mkdir(os.path.join(path_shared, period, report))
        print(f'Successfully create directory "{directory}" and sub-directories')
    except OSError:
        print(f'Creation of the directory "{directory}" failed!')

filepath = os.path.join(path_shared, period, meta)

month = ['A', 'B']
locs = ["Dimond", "Uptown"]
dn = ['Day', 'Night']
for loc in locs:
    os.mknod(os.path.join(filepath, uber, 'Dummie__Uber_'+period+'_'+loc+'.csv'))
    os.mknod(os.path.join(filepath, doordash, 'Dummie__Doordash_'+period+'_'+loc+'.csv'))
    for m in month:
        try:
            os.mknod(os.path.join(filepath, square, 'Dummie__'+period+m+'_'+loc+'.csv'))
        except:
            pass
        try:
            os.mknod(os.path.join(filepath, shift, 'Dummie__'+period+m+'.csv'))
        except:
            pass
        for i in dn:
            os.mknod(os.path.join(filepath, square, 'Dummie__pmethod_'+m+'_'+loc+'_'+i+'.csv'))

for i in locs:
    os.mknod(os.path.join(filepath, square, 'EndofMonth__'+ period + 'N' + '_'+i+'.csv'))
    # os.mknod(os.path.join(filepath, caviar, 'EndofMonth__'+ period + '_' + i + '_caviar_deliver_scrap.csv'))

print('All meta folders created!')