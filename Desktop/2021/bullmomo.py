from cloudquant.interfaces import Strategy


###############################################################################################
# CQ Lite Basic Strategy.
# This public strategy is an example for running a basic momentum based strategy.
# It looks at the closing price for four consecutive 1 minute bars. If they are increasing
# over the period of 6 minutes then the assumption is that it will continue to increase.
#
# There is logic to check to prevent too much loss (stop) and logic to take profits.
# no position is held for more than 15 minutes
#
# This script does produce an output file.
# Warning: Backtesting over multiple days will cause the file to be overwritten.
#
# Pay attention to the __init__ function to see variable names and to change settings.
#
##############################################################################################

class CQ_Basic_Bull_Momentum(Strategy):
    ########################################################################################
    #
    # high level strategy - start/finish
    #
    ########################################################################################

    # called when the strategy starts (aka before anything else)
    @classmethod
    def on_strategy_start(cls, md, service, account):
        pass

    # called when the strategy finish (aka after everything else has stopped)
    @classmethod
    def on_strategy_finish(cls, md, service, account):
        pass

    ########################################################################################
    #
    # symbol universe
    #
    ########################################################################################

    # note that this doesn't start with "self" because it's a @classmethod
    @classmethod
    def is_symbol_qualified(cls, symbol, md, service, account):
        # Return only the symbols that I am interested in trading or researching.
        return symbol in ['EBAY', 'HD', 'MSFT', 'SIRI', 'UNP', 'UPS', 'WBA', 'XLE', 'XLF', 'AAPL']

    # used to load other symbols data not in is_symbol_qualified(). Only used in backtesting
    @classmethod
    def backtesting_extra_symbols(cls, symbol, md, service, account):
        return False

    ########################################################################################
    #
    # start/finish instance related methods
    #
    ########################################################################################

    # used to pass external parameters for each instance (same for values for every instance)
    def __init__(self):  # , **params - if passing in parameters is desired

        self.IsPositionOn = False  # do we have a position on?
        self.LongQty = 0  # long quantity of our position
        self.long_entry_price = 0  # estimated price of our position
        self.TargetProfit = 0.06  # target profit
        self.HowLongHeld = 0  # How Many Bars the position has been held.
        self.filename = "output.csv"
        self.sOutString = ""

    # called at the beginning of each instance
    def on_start(self, md, order, service, account):
        # print "OnStart {0}\t{1}\n".format(service.time_to_string(service.system_time), self.symbol)

        # The model requires that we have at least X minutes of bar data prior
        # to checking to see if a bear price flip has occurred. Therefore we
        # need a variable to track this start time.
        self.model_start = md.market_open_time + service.time_interval(minutes=5)

    # if running with an instance per symbol, call when an instance is terminated
    def on_finish(self, md, order, service, account):
        pass

    ########################################################################################
    #
    # timer method
    #
    ########################################################################################

    # called in timer event is received
    def on_timer(self, event, md, order, service, account):
        pass

    ########################################################################################
    #
    # market data related methods
    #
    ########################################################################################

    # called every minute before the first on_trade of every new minute, or 5 seconds after a new minute starts
    def on_minute_bar(self, event, md, order, service, account, bar):
        #
        # don't want to initiate any long positions in the last 5 minutes of the trading day
        # as we won't likely have time to trade out of the position for a profit using 1 minute
        # bar data.
        #
        if service.system_time < md.market_close_time - service.time_interval(minutes=5, seconds=1):
            #
            # If a position is on we want to check to see if we should take a profit or trade out
            # of a losing position.
            #
            if self.IsPositionOn == True:
                self.HowLongHeld += 1
                # there is a position on, therefore we want to check to see if
                # we should realize a profit or stop a loss
                bar_0 = bar.minute()
                if len(bar_0) > 0:
                    bv_0 = bar_0.low
                    if bv_0[0] > self.long_entry_price + self.TargetProfit:
                        # target profit realized, we want to get out of the position.
                        print ("\texit position now {0}\t{1} entry px at {2} low of bar = {3}\n".format(
                            service.time_to_string(service.system_time), self.symbol, self.long_entry_price, bv_0[0]))

                        self.IsPositionOn = False
                        # send order; use a variable to accept the order_id that order.algo_buy returns
                        sell_order_id = order.algo_sell(self.symbol, "market", intent="exit")

                        self.sOutString = "{0},sellSignal,{1},{2}".format(service.time_to_string(service.system_time),
                                                                          self.symbol, bv_0[0])
                        print (self.sOutString)
                        service.write_file(self.filename, self.sOutString)

                    elif bv_0[0] < self.long_entry_price - self.TargetProfit:
                        # we are losing more than twice our target profit, we therefore
                        # want to stop our losses and trade out of the position.
                        print ("\tExit losing position now {0}\t{1} entry px at {2} low of bar = {3}\n".format(
                            service.time_to_string(service.system_time), self.symbol, self.long_entry_price, bv_0[0]))
                        self.IsPositionOn = False
                        # send order; use a variable to accept the order_id that order.algo_buy returns
                        sell_order_id = order.algo_sell(self.symbol, "market", intent="exit")

                        self.sOutString = "{0},sellSignal,{1},{2}".format(service.time_to_string(service.system_time),
                                                                          self.symbol, bv_0[0])
                        print (self.sOutString)
                        service.write_file(self.filename, self.sOutString)

                if self.HowLongHeld > 15 and self.IsPositionOn == True:  # want to stop our losses and trade out of the position.
                    print ("\tExit too long held position now {0}\t{1} entry px at {2} low of bar = {3}\n".format(
                        service.time_to_string(service.system_time), self.symbol, self.long_entry_price, bv_0[0]))
                    # send order; use a variable to accept the order_id that order.algo_buy returns
                    sell_order_id = order.algo_sell(self.symbol, "market", intent="exit")
                    self.IsPositionOn = False

                    self.sOutString = "{0},sellSignal,{1},{2}".format(service.time_to_string(service.system_time),
                                                                      self.symbol, bv_0[0])
                    print (self.sOutString)
                    service.write_file(self.filename, self.sOutString)

            # we have to have at least 5 minutes of bar data before we can start checking to see if we want to buy.
            else:  # position isn't on, therefore check to see if we should buy.
                if md.L1.timestamp > self.model_start:
                    bar_1 = bar.minute(start=-6, include_empty=True)
                    bv = bar_1.close
                    # Check to see if we have a TD bear flip
                    if len(bar_1.close) == 6:
                        if bv[0] < bv[1] and bv[0] > 0 and bv[1] > 0 and \
                                        bv[1] < bv[2] and bv[2] > 0 and bv[2] < bv[3] and bv[3] > 0 \
                                and bv[3] < bv[4] and bv[3] > 0 and bv[4] < bv[5] and bv[4] > 0 \
                                and bv[0] + (2 * self.TargetProfit) < bv[5]:
                            self.IsPositionOn = True
                            # send order; use a variable to accept the order_id that order.algo_buy returns
                            order_id = order.algo_buy(self.symbol, "market", intent="init", order_quantity=100)
                            print("100 shares of " + self.symbol + " have been ordered!\n")
                            ########################################################################
                            # since on_trade and on_fill aren't called in lite, use the close bar px
                            # as our entry px approximate. We will use this price to check for
                            # profit taking or for stop loss.
                            self.long_entry_price = bv[5];
                            self.sOutString = "{0},buySignal,{1},{2},{3},{4},{5},{6},{7}".format(
                                service.time_to_string(service.system_time), self.symbol, bv[0], bv[1], bv[2], bv[3],
                                bv[4], bv[5])
                            print (self.sOutString)
                            service.write_file(self.filename, self.sOutString)

        else:
            ####################################################################
            # close out of our position at the end of the day because we don't
            # want to carry overnight risk.
            if self.IsPositionOn == True:
                sell_order_id = order.algo_sell(self.symbol, "market", intent="exit")
                self.IsPositionOn = False
                print ("\tExit EOD position now {0} for {1}\n".format(service.time_to_string(service.system_time),
                                                                     self.symbol))

    # called when time and sales message is received
    # NOT CALLED for CloudQuant LITE
    # on_trade is called when a trade is printed in the market data.
    #
    def on_trade(self, event, md, order, service, account):
        pass

    # called when national best bid offer (nbbo) prices change (not size)
    # NOT CALLED for CloudQuant LITE
    #
    # This would be a great place to check for profit or loss changes
    def on_nbbo_price(self, event, md, order, service, account):
        pass

    # called when arca imbalance message is received
    # NOT CALLED for CloudQuant LITE
    def on_arca_imbalance(self, event, md, order, service, account):
        pass

    # called when nasdaq imbalance message is received
    # NOT CALLED for CloudQuant LITE
    def on_nasdaq_imbalance(self, event, md, order, service, account):
        pass

    # called when nyse/amex/nsye mkt/openbook message is received
    # NOT CALLED for CloudQuant LITE
    def on_nyse_imbalance(self, event, md, order, service, account):
        pass

    ########################################################################################
    #
    # order related methods
    #
    ########################################################################################

    # called when order considered pending by the system
    # NOT CALLED for CloudQuant LITE
    def on_ack(self, event, md, order, service, account):
        pass

    # called when an order is rejected (locally or other parts of the order processes e.g. the market)
    # NOT CALLED for CloudQuant LITE
    def on_reject(self, event, md, order, service, account):
        pass

    # called when the position changes in this account and symbol (whether manually or automated)
    # NOT CALLED for CloudQuant LITE
    def on_fill(self, event, md, order, service, account):
        #
        # ######################################################################
        # save the entry price if going long
        # set the IsPoisitionOn flag to True so that we begin to look for ways
        # to trade out of the position
        #
        # not called in Cloud lite!!!!!!!
        pass

    # called when the market has confirmed an order has been canceled
    # NOT CALLED for CloudQuant LITE
    def on_cancel(self, event, md, order, service, account):
        pass
