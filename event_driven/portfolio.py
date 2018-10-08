import datetime
import numpy as np
import pandas as pd
# import Queue

from abc import ABCMeta, abstractmethod
from math import floor
from queue import Queue

from event import FillEvent, OrderEvent
from performance import create_sharpe_ratio, create_drawdowns

class Portfolio(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def update_signal(self, event):
        raise NotImplementedError("Should implement update_signal()")

    @abstractmethod
    def update_fill(self, event):
        raise NotImplementedError("Should implement update_fill()")

class NaivePortfolio(Portfolio):
    def __init__(self, bars, events, start_date, initial_capital=100000.0):
        self.bars = bars
        self.events = events
        self.symbol_list = self.bars.symbol_list
        self.start_date = start_date
        self.initial_capital = initial_capital

        self.all_positions = []
        self.current_positions = dict( (k, v) for k, v in [(s, 0) for s in self.symbol_list])

        self.all_holdings = []
        self.current_holdings = self.construct_current_holdings()

    def construct_current_holdings(self):
        d = dict( (k, v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return d
    
    def update_timeindex(self, event):
        bars = {}
        for sym in self.symbol_list:
            bars[sym] = self.bars.get_latest_bars(sym, N=1)

        dp = dict()
        dp['datetime'] = bars[self.symbol_list[0]]['datetime']
        for s in self.symbol_list:
            dp[s] = self.current_positions[s]

        self.all_positions.append(dp)

        dh = dict()
        dh['datetime'] = bars[self.symbol_list[0]].iloc[0]['datetime']
        dh['cash'] = self.current_holdings['cash']
        dh['commission'] = self.current_holdings['commission']
        dh['total'] = self.current_holdings['cash']

        for s in self.symbol_list:
            dh[s] = self.current_positions[s] * bars[s].iloc[0]['close']    # bars[s][0][5] -> closing price
            dh['total'] += dh[s]

        self.all_holdings.append(dh)

    def update_positions_from_fill(self, fill):
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        elif fill.direction == 'SELL':
            fill_dir = -1
        self.current_positions[fill.symbol] += fill_dir * fill.quantity
        # print ("Current positions: {}".format(self.current_positions[fill.symbol]))

    def update_holdings_from_fill(self, fill):
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        elif fill.direction == 'SELL':
            fill_dir = -1

        fill_cost = self.bars.get_latest_bars(fill.symbol, N=1).iloc[0]['close']
        cost = fill_dir * fill_cost * fill.quantity
        self.current_holdings[fill.symbol] += cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)
        print ("Current holdings: {:.3f}, cash: {:.3f}, total:{:.3f}".format(self.current_holdings[fill.symbol], self.current_holdings['cash'], self.current_holdings['total']))

    def update_fill(self, event):
        assert (event.type == 'FILL'), "The event type should be FILL."
        self.update_positions_from_fill(event)
        self.update_holdings_from_fill(event)

    def generate_naive_order(self, signal):
        order = None

        symbol = signal.symbol
        direction = signal.signal_type
        # strength = signal.strength

        # mkt_quantity = floor(100 * strength)
        mkt_quantity = 100
        cur_quantity = self.current_positions[symbol]
        order_type = 'MKT'

        if direction == 'LONG':
            order = OrderEvent(symbol, order_type, mkt_quantity, 'BUY')
        if direction == 'SHORT':
            order = OrderEvent(symbol, order_type, mkt_quantity, 'SELL')

        # if direction == 'LONG' and cur_quantity == 0:
        #     order = OrderEvent(symbol, order_type, mkt_quantity, 'BUY')
        # if direction == 'SHORT' and cur_quantity == 0:
        #    order = OrderEvent(symbol, order_type, mkt_quantity, 'SELL')

        if direction == 'EXIT' and cur_quantity > 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'SELL')
        if direction == 'EXIT' and cur_quantity < 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'BUY')

        return order

    def update_signal(self, event):
        if event.type == 'SIGNAL':
            order_event = self.generate_naive_order(event)
            self.events.put(order_event)

    def create_equity_curve_dataframe(self):
        # print (self.all_holdings[:5])
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('datetime', inplace=True)
        curve['returns'] = curve['total'].pct_change()
        curve['equity_curve'] = (1.0 + curve['returns']).cumprod()
        self.equity_curve = curve
    
    def output_summary_stats(self):
        self.create_equity_curve_dataframe()
        total_return = self.equity_curve['equity_curve'][-1]
        returns = self.equity_curve['returns']
        pnl = self.equity_curve['equity_curve']

        sharpe_ratio = create_sharpe_ratio(returns)
        max_dd, dd_duration = create_drawdowns(pnl)

        stats = [("Total Return", "%0.2f%%" % ((total_return - 1.0) * 100.0)),
                 ("Sharpe Ratio", "%0.2f" % sharpe_ratio),
                 ("Max Drawdown", "%0.2f%%" % (max_dd * 100.0)),
                 ("Drawdown Duration", "%d" % dd_duration)]
        return stats

        