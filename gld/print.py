import csv
import time
from tabulate import tabulate
from tqdm import tqdm
start = time.time()
def printer(filename):
    print("running printer")
    rows = []
    with open(filename) as f:
        reader = csv.reader(f)
        count = 0
        for row in reader:
            if count == 0:
                rows.append(row)
            if count == 13:
                rows.append(row)
                print(count)
                break
            count += 1
    table_rows = []
    for i in range(len(rows[0])):
        table_rows.append((i, rows[0][i], rows[1][i]))
    headers = ['index', 'param', 'val']
    print('original csv parameters:')
    print(tabulate(table_rows, headers)) #prints all params in original csv files
printer('OptionTradeScreenerResults_20200727.csv')

def never_blank():
    rows = []
    with open('OptionTradeScreenerResults_20200727.csv') as f:
        reader = csv.reader(f)
        count = 0
        blanks = set()
        for row in tqdm(reader):
            if row == [] or row[14] == 'Trade Price':
                continue
            for i in range(len(row)):
                if row[i] == '':
                    blanks.add(i)
    #print("blanks:", blanks)
    never_blanks = []
    for i in range(49):
        if i not in blanks:
            never_blanks.append(i)
    #print("never blank:", never_blanks)
    return never_blanks #finds the never blank params' indices
#never_blank()

def trimmed_printer(): #prints important params
    important = [0, 1, 5, 6, 7, 8, 9, 14, 15, 16, 17, 18, 19, 28, 32, 33, 34, 35, 43, 47]
    rows = []
    with open('OptionTradeScreenerResults_20200727.csv') as f:
        reader = csv.reader(f)
        count = 0
        for row in reader:
            if count == 0:
                rows.append(row)
            if count == 2000:
                rows.append(row)
                print(count)
                break
            count += 1
    table_rows = []
    count = 0
    for i in important:
        table_rows.append((count, rows[0][i], rows[1][i]))
        count += 1
    headers = ['index', 'param', 'val']
    print('trimmed rows parameters:')
    print(tabulate(table_rows, headers)) #prints the never blank params
#trimmed_printer()

#prints an adjustable number of trades from buys_with_scores.csv in tabulate format.
def with_scores_printer():
    with open('buys_with_scores.csv') as f:
        reader = csv.reader(f)
        rows = []
        r = 0
        params = []
        print("printing <100 random buy trades...")
        for row in tqdm(reader):
            r += 1
            if row == []:
                continue


            """
            if row[26] == '6/08' and float(row[32]) < 0:
                params = row
                break
            """
            params = row
            break


            if r % 10000 != 0: #adjustable: number of rows to select. right now every 10,000th row
                continue
            rows.append(row)
        headers = ['Time', 'Symbol', 'Expiry', 'Type', 'Strike', 'Spot', 'OI', 'TrdPrice', 'B.Size', 'Bid', 'Ask', 'A.Size', 'Notional', 'TradeIV',
            '1-day IV Chg', 'IV % Rank', 'Cond.', 'Exec.', 'Delta', 'Spread', '20DayHistIV', 'TradeIV vs 20DayIV (% diff)', '1YearHistIV',
            'TradeIV vs 1YearIV (% diff)', 'Days2Exp.', 'Qty.vsOI', 'Date', 'Spread/Mid', 'B/A.Percentile', '% OTM', '20DayIV / 1YearIV',
            'Score:all', 'Score:top2/3', 'Score:top1/2', 'Score:top1/3', 'NotionalAdj.Score', 'Hist.Prices']
        #print(tabulate(rows, headers))
        #print("# of rows:", len(rows))
        mini_table_rows = []
        for i in range(len(params)):
            mini_table_rows.append((i, headers[i], params[i]))
        mini_table_headers = ['index', 'param', 'val']
        print('params with indexes for buys_with_scores.csv:')
        print(tabulate(mini_table_rows, mini_table_headers))
#with_scores_printer()
for i in range(4):
    print()
print("time elapsed:", time.time() - start)
