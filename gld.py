import csv
import re
from tqdm import tqdm
from print import printer
printer('OptionTradeScreenerResults_20200727.csv')

def screen(days):
    options = {}
    #relative_day = 0
    for d in tqdm(days):
        with open('OptionTradeScreenerResults_20200' + str(d) + '.csv') as f:
            reader = csv.reader(f)
            for row in reader:
                if row == [] or row[1] != 'GLD':
                    continue
                if row[5][-4:] != '2020': #skip if the option expiration isn't in 2020
                    continue
                specific_opt = (row[5], row[6], row[7])
                #relative_time = float(row[0][:2])*10000 + float(row[0][3:5])*100 + float(row[0][6:8]) + relative_day*67000
                if specific_opt in options:
                    options[specific_opt].append((d, row[14]))
                else:
                    options[specific_opt] = [(d, row[14])] #each specific_opt option is mapped to a dictionary d
        #relative_day += 1
    sorted_options = {}
    for opt in sorted(options, key=lambda opt: len(options[opt])):
        sorted_options[opt] = options[opt]
    with open('screened.csv', 'w') as f:
        writer = csv.writer(f)
        for opt in tqdm(sorted_options):
            if len(sorted_options[opt]) <= 10:
                continue
            writer.writerow([opt, sorted_options[opt]])
    print(len(options))
#screen([714, 715, 716, 717, 720, 721, 722, 723, 724, 727])

def option_history():
    with open('screened.csv') as f:
        reader = csv.reader(f)
        for row in reader:
            if row == []:
                continue
            if eval(row[0]) != ('8/21/2020', 'PUT', '175') and eval(row[0]) != ('8/21/2020', 'CALL', '180') :
                continue
            by_days = {}
            for day, price in eval(row[1]):
                if day in by_days:
                    by_days[day].append(price)
                else:
                    by_days[day] = [price] #each price is mapped to the day it was traded
            print(row[0])
            for day in by_days:
                print(day, ': ', by_days[day])
                print()
            print()
            print()
            print()
option_history()
