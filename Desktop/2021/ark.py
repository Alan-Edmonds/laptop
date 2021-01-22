import csv
import time
import os
import statistics as stats
from tqdm import tqdm
from tabulate import tabulate
start = time.time()

#last update: 1/15

#FOR EACH NEW WEEK:
    #redownload holdings csvs
    #copy paste ark trades from emails into ark.csv
    #redownload updated daily price data for the tickers outputted by ticker_etf_tuples()
    #update the date strings in the rows with #UPDATE (ctrl+f)
#the output from ark_PandLs() is two tables of the most bought stocks (since 10/20/2020), one sorted by total buy weight and one sorted by current P/L
#The 'good buys' should have high number for total recent buy weight, no recent sell weight, and ideally also a lowish number for current P/L

#CHOICE: between ark.csv and ark_recent.csv
#tickers to watch from week ending on 1/08: REGN, NVS, EXAS, GOOGL, MGA, API, VEEV, ZM, NFLX, SRPT, BEKE, WDAY, NVDA, BLI, AAPL, JD, TXG


#current_holdings() reads in the current ark holdings. returns dictionary of tuple key = (ticker, etf), float value = weight in etf
#total_traded_weight() calculates total buy and sell weight for each ticker (since data collection started on 10/20/2020). returns dictionary of tuple key = (ticker, etf), list value = [buy weight, sell weight] for tickers with total buy weight > 0.52
#ticker_etf_tuples() prints three tables:
    #top x heaviest current holdings (within individual etfs)
    #top x most sold holdings since 10/20/2020
    #top x most bought holdings since 10/20/2020
    #and returns top x most bought holdings in the dictionary format. These are the tickers that will be input for ark_PandLs(), so the updated daily historical price data csvs need to be downloaded
#single_stock() takes in a (ticker, etf) tuple and reads through ark.csv (csv file containing all ark trades since 10/20/2020, also missing 12/14, 12/15, 12/16).
    #returns two lists of the same size, one representing all ark trades of that tuple, one representing the estimated spot price on the day of each trade
#cost_basis() takes in the trades list and the spot list returned by single_stock(), and splits them into buys and sells. Contains the function calc() which takes in a list of trades and returns the average paid or sold price
    #returns PandL which represents sell basis / cost basis. Returns -999 for stocks where we have no buy trade data for, and returns the most recent spot price for stocks that have not yet been sold.
#ark_PandLs() calculates the PandL for each holding Ark has, and writes it into ark_returns.csv

#NOTE: some tickers show up as 0% for current etf holdings, but have positive buy weight and no sell weight (IPOB, TAK). This must be because they were sold on 12/14, 15, 16.

def current_holdings():
    holdings_dir = 'C:\\Users\\Alan\\Desktop\\December\\holdings\\'
    dict = {}
    for filename in os.listdir(holdings_dir):
        #print(filename)
        with open(holdings_dir + filename) as f:
            reader = csv.reader(f)
            for row in reader:
                if row[1][:3] != 'ARK' or row[3] == '':
                    continue
                dict[(row[3], row[1])] = float(row[7])
    return {k: v for k, v in sorted(dict.items(), key=lambda item: item[1], reverse = True)}

def total_traded_weight():
    dict = {}
    with open('ark.csv') as f: #CHOICE
    #with open('ark_recent.csv') as f:
    #with open('ark_old.csv') as f:
        reader = csv.reader(f)
        for row in reader:
            if (row[4], row[1]) not in dict:
                dict[(row[4], row[1])] = [0, 0]
            if row[3] == 'Buy':
                dict[(row[4], row[1])][0] += float(row[8])
            if row[3] == 'Sell':
                dict[(row[4], row[1])][1] += -1*float(row[8])
    return {k: v for k, v in sorted(dict.items(), key=lambda item: item[1][0], reverse = True)} #ticker/etf tuples sorted from most amount bought to least amount bougth

def ticker_etf_tuples(print_bool):
    holdings_dict = current_holdings()
    traded_weights_dict = total_traded_weight()
    for key in traded_weights_dict:
        if key in holdings_dict:
            traded_weights_dict[key] = [holdings_dict[key], traded_weights_dict[key][0], traded_weights_dict[key][1]]
        else:
            traded_weights_dict[key] = [0, traded_weights_dict[key][0], traded_weights_dict[key][1]]
    if print_bool:
        print()
        print()
        print()
        print('20 most heavily weighted stocks in individual etfs:')
        print()
        headers = ['stock', 'etf', 'current % of etf', 'total traded weight in buys', 'total traded weight in sells']
        table_rows = []
        for key in holdings_dict:
            if len(table_rows) > 20:
                break
            if key in traded_weights_dict:
                table_rows.append([key[0], key[1], traded_weights_dict[key][0], traded_weights_dict[key][1], traded_weights_dict[key][2]])
            else:
                table_rows.append([key[0], key[1], holdings_dict[key], 0, 0]) #this is for current holdings that havent been bought/sold since 10/20/2020
        print(tabulate(table_rows, headers))
        print()
        print()
        print()
        print('20 most weight sold since 10/20/2020 (or 12/17/2020):')
        print()
        headers = ['stock', 'etf', 'current % of etf', 'total traded weight in buys', 'total traded weight in sells']
        table_rows = []
        traded_weights_dict = {k: v for k, v in sorted(traded_weights_dict.items(), key=lambda item: item[1][2], reverse = False)}
        for key in traded_weights_dict:
            if len(table_rows) > 20:
                break
            table_rows.append([key[0], key[1], traded_weights_dict[key][0], traded_weights_dict[key][1], traded_weights_dict[key][2]])
        print(tabulate(table_rows, headers))
        print()
        print()
        print()
        print('most weight bought since 10/20/2020 (or 12/17/2020):')
        print()
    headers = ['stock', 'etf', 'current % of etf', 'total traded weight in buys', 'total traded weight in sells']
    table_rows = []
    topbuys = {}
    traded_weights_dict = {k: v for k, v in sorted(traded_weights_dict.items(), key=lambda item: item[1][1], reverse = True)}
    for key in traded_weights_dict:
        if traded_weights_dict[key][1] < 0.52:
            break
        #if key[0] not in ['RHHBY', 'TCEHY']: #CHOICE
        if key[0] not in []:
            topbuys[key] = traded_weights_dict[key]
        table_rows.append([key[0], key[1], traded_weights_dict[key][0], traded_weights_dict[key][1], traded_weights_dict[key][2]])
    if print_bool:
        print(tabulate(table_rows, headers))
        print()
        print()
        print()
    return topbuys
ticker_etf_tuples(True)

def single_stock(tuple, print_bool = False):
    trades = []
    with open('ark.csv') as f: #CHOICE
    #with open('ark_recent.csv') as f:
    #with open('ark_old.csv') as f:
        reader = csv.reader(f)
        for row in reader:
            if (row[4], row[1]) != tuple:
                continue
            trades.append((row[4], float(row[8]), row[2], row[1], row[3]))
    trades2 = []
    for _ in trades:
        trades2.append(_)
    spot = []
    previousday = None
    dir = 'C:\\Users\\Alan\\Desktop\\December\\stonks\\'
    with open(dir + tuple[0] + '_daily_historical-data-01-15-2021.csv') as f: #UPDATE
        reader = csv.reader(f)
        for row in reader:
            day = row[0].replace('/0', '/')
            if day == trades[0][2]:
                spot.append(round((float(row[1]) + float(row[2]) + float(row[3]) + float(row[4]))/4, 3))
                just_appended = trades.pop(0)[2]
                if len(trades) == 0:
                    break
                if trades[0][2] == just_appended:
                    #the ticker was traded twice in the same day
                    spot.append(round((float(row[1]) + float(row[2]) + float(row[3]) + float(row[4]))/4, 3))
                    trades.pop(0)
                    if len(trades) == 0:
                        break
    headers = ['ticker', '% of etf', 'date', 'fund', 'buy/sell', 'spot']
    table_rows = []
    #print(len(trades2), len(spot))
    for i in range(len(trades2)):
        table_rows.append([trades2[i][0], trades2[i][1], trades2[i][2], trades2[i][3], trades2[i][4], spot[i]])
    if print_bool:
        print()
        print()
        print(tabulate(table_rows, headers))
    return trades2, spot

def cost_basis(trades, spot, print_bool):
    buys, buyspot = [], []
    sells, sellspot = [], []
    for i in range(len(trades)):
        if trades[i][4] == 'Buy':
            buys.append(trades[i])
            buyspot.append(spot[i])
        else:
            assert trades[i][4] == 'Sell'
            sells.append(trades[i])
            sellspot.append(spot[i])
    def calc(trades, spot, buy_bool):
        total_weight = 0
        for trade in trades:
            total_weight += float(trade[1])
        cost_basis = 0
        assert len(trades) == len(spot)
        for i in range(len(trades)):
            cost_basis += trades[i][1]/total_weight * spot[i]
        if print_bool:
            print()
            if buy_bool:
                print('cost basis for ', trades[0][0], ': ', cost_basis, 'representing ', total_weight, 'percent of etf')
            else:
                assert not buy_bool
                print('sell basis for ', trades[0][0], ': ', cost_basis, 'representing ', total_weight, 'percent of etf')
        return cost_basis
    PandL = 1
    if len(buys) == 0:
        return -999
    if len(buys) > 0:
        PandL /= calc(buys, buyspot, True)
    if len(sells) > 0:
        PandL *= calc(sells, sellspot, False)

    #this block (always) appends the current spot price to sells and sellspot lists
    dir = 'C:\\Users\\Alan\\Desktop\\December\\stonks\\'
    with open(dir + trades[0][0] + '_daily_historical-data-01-15-2021.csv') as f: #UPDATE
        reader = csv.reader(f)
        for row in reader:
            if row[1] == 'Open':
                continue
            PandL *= round((float(row[1]) + float(row[2]) + float(row[3]) + float(row[4]))/4, 1)
            break

    return 100*(PandL-1)


def ark_PandLs():
    dir = 'C:\\Users\\Alan\\Desktop\\December\\stonks\\'
    already_downloaded = set()
    not_downloaded = set()
    for filename in os.listdir(dir):
        i = filename.index('_')
        already_downloaded.add(filename[:i])
    dict = ticker_etf_tuples(False)
    for key in dict:
        if key[0].lower() not in already_downloaded:
            not_downloaded.add(key[0])
    withPandLs = {}
    def function_caller(ticker, etf):
        trades, spot = single_stock((ticker, etf), False)
        return cost_basis(trades, spot, False)
    for tuple in tqdm(dict):
        if tuple[0] in not_downloaded:
            continue
        withPandLs[tuple] = [dict[tuple][0], dict[tuple][1], dict[tuple][2], function_caller(tuple[0], tuple[1])]
    #print(PandLs)
    withPandLs = {k: v for k, v in sorted(withPandLs.items(), key=lambda item: item[1][3], reverse = False)}
    headers = ['stock', 'etf', 'current % of etf', 'total traded weight in buys', 'total traded weight in sells', 'current P/L']
    table_rows = []
    with open('ark_returns.csv', 'w') as f_write:
        writer = csv.writer(f_write)
        writer.writerow(['stock', 'etf', 'current % of etf', 'total traded weight in buys', 'total traded weight in sells', 'current P/L'])
        for key in withPandLs:
            if withPandLs[key][3] > -999:
                writer.writerow([key[0], key[1], withPandLs[key][0], withPandLs[key][1], withPandLs[key][2], withPandLs[key][3]])
                table_rows.append([key[0], key[1], withPandLs[key][0], withPandLs[key][1], withPandLs[key][2], withPandLs[key][3]])
    print(tabulate(table_rows, headers))
    print()
    print()
    print()
    sortedbuyweight = {k: v for k, v in sorted(withPandLs.items(), key=lambda item: item[1][1], reverse = True)}
    table_rows = []
    for key in sortedbuyweight:
        if sortedbuyweight[key][3] > -999:
            table_rows.append([key[0], key[1], sortedbuyweight[key][0], sortedbuyweight[key][1], sortedbuyweight[key][2], sortedbuyweight[key][3]])
    print(tabulate(table_rows, headers))
    print()
    print()
    print()
ark_PandLs()

print()
print('time elapsed:', time.time() - start)
