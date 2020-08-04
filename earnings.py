import csv
import time
import matplotlib.pyplot as plot
import statistics as stats
import numpy
from tqdm import tqdm
from original_combiner import all_combiner
start = time.time()
#   go to https://www.investing.com/earnings-calendar/
#   select desired timeframe and scroll down fully to load all text
#   ctrl+A and copy paste into csv file
specials = ['SPY', 'QQQ', 'IWM', 'SLV', 'XLF', 'GLD', 'GDX', 'VXX', 'TSLA', 'USO']
all_tradedays = ['0505', '0507', '0508', '0511', '0512', '0513', '0514', '0515', '0518', '0519', '0520', '0521',
    '0522', '0526', '0527', '0528', '0529', '0601', '0602', '0603', '0604', '0605', '0608', '0609', '0610',
    '0611', '0612', '0615', '0616', '0617', '0618', '0619', '0622', '0623', '0624', '0625', '0626', '0629',
    '0630', '0701', '0702']
#all_combiner(all_tradedays) #runtime ~2min

def well_traded(): #a cleaner function that reads in the earnings_calendar.csv file and creates the earnings_well_traded.csv
    dictionary = {} #dictionary mapping file for each date to the set of tickers traded on that date
    print("creating tradeday - tickers dictionary...")
    for d in tqdm(all_tradedays):
        ticker_set = set()
        with open('OptionTradeScreenerResults_2020' + d + '.csv') as f:
            reader = csv.reader(f)
            for row in reader:
                ticker_set.add(row[1])
        dictionary[d] = ticker_set

    print("cleaning data and writing earnings_well_traded.csv...")
    data_to_write = []
    earnings_date = ''
    with open('earnings_calendar.csv') as f:
        reader = csv.reader(f)
        for row in tqdm(reader):
            if len(row) == 3:
                i = row[1].rindex(' ')
                month = row[1][:i]
                if float(row[1][i+1:]) < 10: #converts 507 to 0507
                    day = '0' + row[1][i+1:]
                else:
                    day = row[1][i+1:]
                if month == ' May':
                    earnings_date = '05' + day
                elif month == ' June':
                    earnings_date = '06' + day
                elif month == ' July':
                    earnings_date = '07' + day
                elif month == ' August':
                    earnings_date = '08' + day
                continue
            for i in range(len(row)): #this loop exists because of weird shit have to deal with in the earnings_calendar.csv file
                if earnings_date not in all_tradedays: #this keeps us from writing future earnings dates into earnings_well_traded.csv
                    continue
                if '(' in row[i]:
                    first_parenthesis = row[i].index('(')
                    second_parenthesis = row[i].index(')')
                    ticker = row[i][first_parenthesis + 1 : second_parenthesis]
                    if '_' in ticker:
                        break #removing tickers with annoying _'s cause fuck em
                    #the following lines add the [earnings_date, ticker] row if it was traded in the five trading days leading up to earnings
                    i = all_tradedays.index(earnings_date) #the index (in above list 'dates') of the date for earnings of this row's ticker
                    well = True
                    for d in [all_tradedays[i-5], all_tradedays[i-4], all_tradedays[i-3], all_tradedays[i-2], all_tradedays[i-1]]:
                        if ticker not in dictionary[d]:
                            well = False
                    if well:
                        data_to_write.append([earnings_date, ticker])
                    break
    with open('earnings_well_traded.csv', 'w') as f:
        writer = csv.writer(f)
        for ticker in specials:
            writer.writerow(['0000', ticker])
        for row in data_to_write:
            writer.writerow(row)
#well_traded() #runtime ~30sec

def process_options():
    #   writes earnings_processed_options.csv. row[0] is earnings date, row[1] is specific option, row[2] is a dictionary mapping
    #   trade days to tuples, where tuple[0] is the avg price of the speficic option on that day, and tuple[1] is the avg spot price
    #   of the stock for that day.
    data_to_write = []
    with open('earnings_well_traded.csv') as f1:
        reader1 = csv.reader(f1)
        print("reading in earnings_well_traded.csv...")
        for earnings_row in tqdm(reader1):
            if earnings_row == []:
                continue
            options = {} #dictionary of dictionaries: specific options each mapped to their dictionary which maps tradeday to a list of option's prices on that day
            with open('all_combiner.csv') as f2:
                reader2 = csv.reader(f2)
                print()
                print()
                print("reading in all_combiner.csv and finding satisfying options trades for ", earnings_row, "...")
                current = (None, None)
                count = 0
                limit_reached = []
                for row in tqdm(reader2):
                    if row == []:
                        continue
                    if row[2][-4:] != '2020': #skip if the option expiration isn't in 2020
                        continue
                    if row[1] == earnings_row[1]: #check that the option trade is for the ticker in question
                        if row[1] in specials: #these conditionals prevent us from looking at >5000 rows from all_combiner if they are from the same ticker and trade day. For example, >60000 SPY trades happened on 06/05
                            ticker_and_tradeday = (row[1], row[26])
                            if count == 5000:
                                limit_reached.append(ticker_and_tradeday)
                                count = 0
                            if ticker_and_tradeday in limit_reached:
                                continue
                            if ticker_and_tradeday == current:
                                count += 1
                            else:
                                current = ticker_and_tradeday
                                count = 1
                        specific_opt = (row[1], row[2], row[3], row[4])
                        if specific_opt in options:
                            if row[26] in options[specific_opt]:
                                options[specific_opt][row[26]].append(row[7])
                            else:
                                options[specific_opt][row[26]] = [row[7]] #each dictionary d is a mapping of tradeday to a tuple of two lists; list of the option's prices on that day, and list of corresponding spot prices
                        else:
                            options[specific_opt] = {row[26] : [row[7]]} #each specific_opt option is mapped to a dictionary d
            for specific_opt in options:
                inner_dict = options.get(specific_opt)
                if len(inner_dict) <= 3: #option must have data for being traded on >3 different trading days
                    continue
                if specific_opt[0] not in specials:
                    day_before_earnings = all_tradedays[all_tradedays.index(earnings_row[0]) - 1]
                    if day_before_earnings not in inner_dict: #skips the specific_opt if I don't have data for it being traded on the day before earnings
                        continue
                    if list(inner_dict.keys()).index(day_before_earnings) <= 1: #index of day_before_earnings has to be at least 2. Skips the specific_opt if I don't have data for it being traded on two different trading days before day_before_earnings. In other words, the option has to have date for it being traded on the three trading days before earings.
                        continue
                simplified_inner_dict = {} #each specific_opt has a corresponding simplified_inner_dict. In this dictionary, each tradeday is mapped to the avg option price for that day.
                for tradeday in inner_dict:
                    price_history_strings = inner_dict.get(tradeday)
                    floats = []
                    for price in price_history_strings:
                        floats.append(float(price))
                    simplified_inner_dict[tradeday] = round(stats.mean(floats), 3)
                data_to_write.append((earnings_row[0], specific_opt, simplified_inner_dict))
                print(earnings_row[0], specific_opt, simplified_inner_dict)
    print("writing data into earnings_processed_options.csv...")
    with open('earnings_processed_options.csv', 'w') as f:
        writer = csv.writer(f)
        for row in tqdm(data_to_write):
            writer.writerow(row)
#process_options() #runtime ~90min

def avg_spot_prices(): #calculates avg_spot_prices on each tradeday for each of the tickers in earnings_processed_options.csv
    tickers_and_dates = {} #maps each ticker from the options earnings_processed_options.csv to the list of trade days for that ticker's options
    with open('earnings_processed_options.csv') as f:
        reader = csv.reader(f)
        print("reading in earnings_processed_options.csv and creating tickers_and_dates dictionary...")
        for row in tqdm(reader):
            if row == []:
                continue
            if eval(row[1])[0] in tickers_and_dates:
                for d in eval(row[2]).keys():
                    tickers_and_dates[eval(row[1])[0]].add(d)
            else:
                date_set = set()
                for d in eval(row[2]).keys():
                    date_set.add(d)
                tickers_and_dates[eval(row[1])[0]] = date_set
    #runtime for code above is ~0 seconds
    print("using tickers_and_dates dictionary to read csv files...")
    data_to_write = []
    for ticker in tqdm(tickers_and_dates):
        dict = {} #list of (date, avg spot price) tuples for the ticker
        for date in sorted(list(tickers_and_dates.get(ticker))):
            prices = []
            with open('OptionTradeScreenerResults_2020' + date + '.csv') as f_:
                reader = csv.reader(f_)
                for row in reader:
                    if row == []:
                        continue
                    if row[1] == ticker:
                        prices.append(float(row[8]))
            prices.sort()
            outliers_index = round(len(prices)/10)
            if outliers_index == 0:
                dict[date] = round(stats.mean(prices), 3)
            else: #accounts for outliers in spot price, sometimes an $80 stock has spot price value of like 0.17 randomly
                dict[date] = round(stats.mean(prices[outliers_index : -1*outliers_index]), 3)
        data_to_write.append([ticker, dict])
    with open('earnings_avg_spot_prices.csv', 'w') as f:
        writer = csv.writer(f)
        for row in data_to_write:
            writer.writerow(row)
avg_spot_prices() #runtime ~60min

def find_strangles_helper():
    #   Reads in rows from earnings_avg_spot_prices.csv. row[0] is the ticker and row[1] is its dictionary where keys are
    #   tradedays and the corresponding value is avg.spot.price for the ticker on that trade day. For each ticker/tradeday combination,
    #   we read in earnings_processed_options.csv and create the 'calls' and 'puts' dictionaries. For 'calls', the key is option expiry
    #   and the corresponding value is the list of call strike prices for options of that expiry
    data_to_write = []
    print("running find_strangles_helper()...")
    with open('earnings_avg_spot_prices.csv') as f:
        reader = csv.reader(f)
        for avg_spot_row in tqdm(reader):
            if avg_spot_row == []:
                continue
            print("   ...", avg_spot_row[0])
            for tradeday in eval(avg_spot_row[1]).keys():
                calls = {} #dictionary mapping each expiry to a list of the different strikes traded for call options with that expiry
                puts = {} #same but for expiry_strikes_puts
                with open('earnings_processed_options.csv') as f_:
                    reader_ = csv.reader(f_)
                    for row in reader_:
                        if row == []:
                            continue
                        if eval(row[1])[0] != avg_spot_row[0] or tradeday not in eval(row[2]).keys(): #the specific option represented by row is not for the ticker in question from avg_spot_row, or it was not traded on tradeday
                            continue
                        expiry = eval(row[1])[1]
                        type = eval(row[1])[2]
                        strike = eval(row[1])[3]
                        if len(expiry) == 8: #changes 5/8/2020 to 5/08/2020 to deal with sorting issues
                            #print('shortlength')
                            expiry = expiry[:2] + '0' + expiry[2:]
                        if len(expiry) == 9: #changes 6/08/2020 to 06/08/2020
                            expiry = '0' + expiry
                        if type == 'CALL':
                            if expiry in calls: #if this expiry already has been added to calls, we add the new strike price to the list of strike prices for that expiry
                                calls[expiry].append(strike)
                            else: #if this expiry has not yet been added to calls
                                calls[expiry] = [strike]
                        elif type == 'PUT':
                            if expiry in puts:
                                puts[expiry].append(strike)
                            else:
                                puts[expiry] = [strike]
                ordered_calls = {}
                ordered_puts = {}
                for k in sorted(calls):
                    ordered_calls[k] = calls.get(k)
                for k in sorted(puts):
                    ordered_puts[k] = puts.get(k)
                data_to_write.append((avg_spot_row[0], tradeday, ordered_calls, ordered_puts))
                print(avg_spot_row[0], tradeday, ordered_calls, ordered_puts)
    with open('earnings_strangles_helper.csv', 'w') as f:
        writer = csv.writer(f)
        for row in data_to_write:
            writer.writerow(row)
#find_strangles_helper() #runtime ~3min

def find_strangles():
    # Uses earnings_strangles_helper.csv in combination with earnings_avg_spot_prices.csv to identify call/put pairs that
    #   could qualify as a strangle. Right now, a call/put pair where the call is X %otm and the put is Y %otm qualifies
    #   if abs(X - Y) is less than a certain value C, currently set at 6.66 (adjustable). In other words, the pair qualifies
    #   if the difference in the %otm for call and put is < C.
    # We then write each qualified strangle into earnings_strangles.csv, along with several relevant stats
    strangles = []
    print("creating avg spot price dictionary...")
    avg_spot_price_dictionary = {} #this dictionary maps each ticker to the dictionary in earnings_avg_spot_prices.csv (which maps each tradeday to the avg spot price of the ticker on that day)
    with open('earnings_avg_spot_prices.csv') as f:
        reader = csv.reader(f)
        for row in tqdm(reader):
            if row == []:
                continue
            avg_spot_price_dictionary[row[0]] = eval(row[1])
    print("finding strangles...")
    with open('earnings_strangles_helper.csv') as f:
        reader = csv.reader(f)
        for row in tqdm(reader):
            if row == []:
                continue
            avg_spot_price = float(avg_spot_price_dictionary.get(row[0]).get(row[1]))
            calls = eval(row[2])
            puts = eval(row[3])
            good_expiries = [] #list of expiries that appear in both calls and puts
            for expiry in puts:
                if expiry in calls:
                    good_expiries.append(expiry)
            for expiry in good_expiries:
                call_strikes = calls.get(expiry)
                put_strikes = puts.get(expiry)
                for c_strike in call_strikes:
                    c_percent_otm = round(100*(float(c_strike) - avg_spot_price)/avg_spot_price, 3)
                    for p_strike in put_strikes:
                        p_percent_otm = round(100*(avg_spot_price - float(p_strike))/avg_spot_price, 3)
                        otm_difference = round(abs(c_percent_otm - p_percent_otm), 3) #differnce in c_strike's %otm and p_strike's %otm
                        otm_midpoint = round(c_percent_otm/2 + p_percent_otm/2, 3) #avg of c_strike's %otm and p_strike's %otm. Only relevant if otm_difference is a small value.
                        if otm_difference < 6.66: #this is a very liberal cutoff and allows some ridiculous strangles for high priced stocks. It was set as a conservative cutoff for strangles on NIO, whose spot price is betwee 3.5 and 4. It is much too liberal a cutoff for stocks like BABA whose spot prices are >200
                            strangles.append([row[1], row[0], avg_spot_price, otm_difference, otm_midpoint, (expiry, c_strike, c_percent_otm, p_strike, p_percent_otm)])
    with open('earnings_strangles.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['TradeDay', 'Ticker', 'AvgSpotPrice', 'otm_difference', 'otm_midpoint', '(expiry, c_strike, c_%otm, p_strike, p_%otm)'])
        for row in strangles:
            writer.writerow(row)
#find_strangles() #runtime <1 second

def analyze_strangles1():
    #   Simple enough function that looks at the strangles in earnings_strangles.csv and calculates their price change over time,
    #   taking earnings date into account. The harder part will be to only look at the most important strangles. This will largely
    #   be determined by how 'balanced' the strangle is, but another important factor is how well traded the strangle is.
    #   For example, maybe 'ALAN' traded at $43 for ten days, so the most balanced strangle is call45/put40. However,
    #   maybe the call45 was only traded on three of the ten days, and call50 was traded on eight of the ten days, so it may be good
    #   to look at the call50/put40 strangle as well or instead.

    #IDEAS: to start off, we can look at the price change over time for all of the strangles in earnings_strangles.csv.
    #   In order to hone down this function, the first filter could be based on the number of times that the strangle was traded
    #   before earnings. One that is traded 10 times before earnings is more valuable than one traded once before earnings.
    #   The second filter coudl be based on balanced-ness of the strangle, touched on above, but this is actually not too important
    #   as the changing avg.spot.price of the stocks will mean that none of these strangles should stay balanced for long. This
    #   function achieves a similar goal to analyze_strangles2(), with the distinction being that price changes in the stock should
    #   be reflected in, or at least influence, the price change of the specific strangles we look at in this function.
    None






analyze_strangles1()



def analyze_strangles2():
    # A more sophisticated function that essentially calculates changing IV for each ticker. Instead of seeing how specific
    #   strangles' prices changed, look at how difference %otm/expiry combination strangles performed, taking the movement of the
    #   stock's avg.spot.price into account. For example, say 'ALAN' jumped to $60 on day 11, and earnings is on day 15. If we want
    #   to see how IV has been changing, the call45/put40 strangle for the first ten days would be comparable to a call65/put55,
    #   and the call50/put40 would be comparable to a call70/put55.
    # Compared to analyze_strangles1(), this does not need to focus as much on whether the strangle was well traded, because
    #   we will use predetermined values for %otm/expiry in order to find strangles worth analyzing. The harder part will be
    #   detemining how to choose these %otm/expiry values in order to get useful results from this function. I would like to choose
    #   these values so that this %otm has strangles on almost every available tradeday (taking each day's avg.spot.price into account).
    #   Of course, the %otm value will have to allow for leeway. In the example, call45 on day 10 is 4.7% otm, but the most comparable
    #   strike of call65 on day11 is 8.3% otm, which is barely a better comparison than call60 which would have been 0% otm.

    #IDEAS: this seems like it will be simpler than analyze_strangles1(). earnings_strangles.csv already has the call and put %otm values,
    #   so a simple way might be to find one or several (approx.call%otm, approx.put%otm) tuples that appear often for a certain expiry
    #   (for a certain ticker). Once this is done, the change in price of this %otm/expiry combo demonstrates IV change.
    None
analyze_strangles2()

#things to look at:
#   open interest of option vs stock volume
#   price movement of the stock itself: can look at strangles of a certain %otm, and see how, for example, a 10%otm strangle on 5/08 cost vs 10%otm strangle on 6/08 for a company with earnings on 6/09
#   standard to be compared to should be a comparable spy/vix/qqq strangle on the same time frame.
#   can also look at how a specific strangle performed from 5/08 to 6/08 without taking all factors into account (keeping strike prices the same throughout, maybe even for a stock with lots of price movement leading up to earnings)
#   in the end, try to use intractable algos to find ways for predicting which stocks will experience the best iv surges before earnings. will have to use lots of relevant data, possibly including price movement of stock leading
#       up to earnings, volume of stock, open interest of stock contracts vs normal stock volume, short interest, general state of market/vix, 'hype level' of the stock (how big of a name it is, media attention/twitter mentions?),
#   Even further, it will benefit to know which strangle will work best. For example, sometimes a close atm strangle doesn't gain much leading up to earnings but the far otm strangle gains immensely, or vice versa.
#       to build upon that, we can go further by not looking just at %otm but also at expiry dates. Maybe even will be able to identify if a certain expiry date's volatility hasnt caught up with the rest of the options and is
#       thus underpriced. For example, maybe earnings are on july1st, and the july10 options have increased by 50% in past weeek, but july17 options only increased by 30%




print("time elapsed:", time.time() - start)
