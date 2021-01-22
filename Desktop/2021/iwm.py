import csv
import time
import os
import statistics as stats
from tqdm import tqdm
from tabulate import tabulate
start = time.time()

#writes csv of daily data since 2012 in chronological order
def reverse_csv(dir, filename, newfilename):
    with open(dir + newfilename, 'w') as f_write:
        writer = csv.writer(f_write)
        writer.writerow(['Time','Open','High','Low','Last','Change','%Chg','Volume'])
        rows_to_write = []
        with open(dir + filename) as f:
            reader = csv.reader(f)
            for row in reader:
                if row[0] == 'Time':
                    continue
                if float(row[0][-4:]) < 2012:
                    break
                rows_to_write.insert(0, row)
        for row in tqdm(rows_to_write):
            writer.writerow(row)
downloaded = 'iwm_daily_historical-data-01-22-2021.csv' #UPDATE
reverse_csv('C:\\Users\\Alan\\Desktop\\cloudquant\\csvs\\', downloaded, 'iwm_daily.csv')

def write_dataset():
    dir = 'C:\\Users\\Alan\\Desktop\\cloudquant\\csvs\\'
    with open(dir + 'iwm_dataset.csv', 'w') as f_write:
        writer = csv.writer(f_write)
        writer.writerow([ #labels
            'Time','Open','High','Low','Last','Change','%Chg','Volume',
            '1day %chg', '2day %chg', '3day %chg', '4day %chg', '5day %chg', '9day %chg', '13day %chg', '20day %chg', #1day is change from open price to previous close
            '% of 1day range', '% of 2day range', '% of 3day range', '% of 4day range', '% of 5day range', '% of 9day range', '% of 13day range', '% of 20day range', #on a period of past x trading days with lowest low of 95 and highest high of 105, our half-range is 5 and our 'neutral' price is 100. An open at 101 would be +20%, open at 110 would be +200%, open at 100 would be 0%, open at -94 would be -120%
            '1day high %above', '2day high %above', '3day high %above', '4day high %above', '5day high %above', '9day high %above', '13day high %above', '20day high %above',
            '1day low %above', '2day low %above', '3day low %above', '4day low %above', '5day low %above', '9day low %above', '13day low %above', '20day low %above',
            '1ma %above', '2ma %above', '3ma %above', '4ma %above', '5ma %above', '9ma %above', '13ma %above', '20ma %above', '49ma %above',  #2ma is hlc3 ma for previous 2 trading days
            '1vwap %above', '2vwap %above', '3vwap %above', '4vwap %above', '5vwap %above', '9vwap %above', '13vwap %above', '20vwap %above', '49vwap %above',
            #'1day relative volume', '2day relative volume', '3day relative volume', '4day relative volume', '5day relative volume', '9day relative volume', '13day relative volume', '20day relative volume', '49day relative volume',
            #'1day relative range', '2day relative range', '3day relative range', '4day relative range', '5day relative range', '9day relative range', '13day relative range', '20day relative range', '49day relative range'
        ])
        def features_to_write(_50days, todays_open):
            features = [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]]
            for x in [1, 2, 3, 4, 5, 9, 13, 20]: #this appends x day %change
                x_close = _50days[x][2]
                features.append(round((todays_open - x_close)/x_close*100, 5))

            #these three are easy to do at the same timem, using lists to maintain order for features
            range_list = []
            high_list = []
            low_list = []
            for x in [1, 2, 3, 4, 5, 9, 13, 20]: #this appends '% of xday range', 'xday high %above', 'xday low %above'
                high = -1
                low = 9999
                for i in range(x):
                    i += 1
                    if _50days[i][0] > high:
                        high = _50days[i][0]
                    if _50days[i][1] < low:
                        low = _50days[i][1]
                half_range = (high - low)/2
                center = low + half_range
                range_list.append(round((todays_open - center)/half_range*100, 5))
                high_list.append(round((todays_open - high)/high*100, 5))
                low_list.append(round((todays_open - low)/low*100, 5))
            for x in range_list:
                features.append(x)
            for x in high_list:
                features.append(x)
            for x in low_list:
                features.append(x)

            ma_list = []
            vwap_list = []
            for x in [1, 2, 3, 4, 5, 9, 13, 20, 49]: #this appends '% above x-ma (hlc3)'
                price_sum = 0
                volume_sum = 0
                pv_sum = 0
                for i in range(x):
                    i += 1
                    hlc3 = (_50days[i][0] + _50days[i][1] + _50days[i][2])/3
                    price_sum += hlc3
                    volume_sum += _50days[i][3]
                    pv_sum += hlc3*_50days[i][3]
                ma = price_sum/x
                vwap = pv_sum/volume_sum
                ma_list.append(round((todays_open - ma)/ma*100, 5))
                vwap_list.append(round((todays_open - vwap)/vwap*100, 5))
            for x in ma_list:
                features.append(x)
            for x in vwap_list:
                features.append(x)

            return features

        with open(dir + 'iwm_daily.csv') as f:
            reader = csv.reader(f)
            _50days = [] #data from past 50 days, most recent at the front
            for row in reader:
                if row == [] or row[0] == 'Time':
                    continue
                _50days.insert(0, (float(row[2]), float(row[3]), float(row[4]), float(row[7]))) #tuple of (high, low, close, volume)
                if len(_50days) < 50:
                    continue
                _50days = _50days[:50]
                todays_open = float(row[1])
                writer.writerow(features_to_write(_50days, todays_open))
write_dataset()

def index_stuff(): #doenst do anything important, just for reading csv and looking up indexes of certain variables
    with open('C:\\Users\\Alan\\Desktop\\cloudquant\\csvs\\Trades - small.csv') as f:
        reader = csv.reader(f)
        count = 0
        for row in reader:
            if count > 1:
                break
            for i in range(len(row)):
                if row[i] == 'entry_collect':
                    print(i, row[i])
                if row[i] == 'entry_time':
                    print(i, row[i])
                if row[i] == 'entry_price':
                    print(i, row[i])
                if row[i] == 'exit_price':
                    print(i, row[i])
                if row[i] == 'entry_pl':
                    print(i, row[i])
            print('1', row[1])
            print('30', row[30])
            print('31', row[31])
            print('34', row[34])
            print('41', row[41])
            print()
            count += 1
#index_stuff()

def trades_dailydata():
    with open('C:\\Users\\Alan\\Desktop\\cloudquant\\csvs\\Trades - small.csv') as f:
        reader = csv.reader(f)
        for row in reader:
            """"


print()
print('time elapsed:', time.time() - start)
