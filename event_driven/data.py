import datetime
import os, os.path
import pandas as pd

from abc import ABCMeta, abstractmethod

from event import MarketEvent


class DataHandler(object):
    def get_latest_bars(self, symbol, N=1):
        raise NotImplementedError("Should implement get_latest_bars()")

    def update_bars(self):
        raise NotImplementedError("Should implement update_bars()")

class HistoricCSVDataHandler(DataHandler):
    def __init__(self, events, csv_dir, symbol_list):
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True

        self._open_convert_csv_file()

    def _open_convert_csv_file(self):
        comb_index = None
        for s in self.symbol_list:
            self.symbol_data[s] = pd.io.parsers.read_csv(
                os.path.join(self.csv_dir, "{}.csv".format(s)),
                header=0, index_col=0,
                names=['datetime', 'open', 'low', 'high', 'close', 'volume', 'close']
            )

            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            self.latest_symbol_data[s] = []

        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index, method='pad').iterrows()


    def _get_new_bar(self, symbol):
        for b in self.symbol_data[symbol]:
            yield tuple([symbol, datetime.datetime.strptime(b[0], '%Y-%m-%d %H:%M:%S'), b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])

    def get_latest_bars(self, symbol, N=1):
        try:
            bars_list = self.latest_symbol_data[symbol]
        except:
            print ("That symbol is not available in the historical data set.")
        else:
            return bars_list[-N:]

    def update_bars(self):
        for s in self.symbol_list:
            try:
                # bar = self._get_new_bar(s).next()  # python 2
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)

        self.events.put(MarketEvent())

class HistoricPDDataHandler(DataHandler):
    def __init__(self, events, csv_dir, symbol_list):
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True

        self._open_convert_csv_file()

    def _open_convert_csv_file(self):
        comb_index = None
        for s in self.symbol_list:
            '''
            self.symbol_data[s] = pd.io.parsers.read_csv(
                os.path.join(self.csv_dir, "{}.csv".format(s)),
                header=0, index_col=0,
                names=['datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'close']
            )
            '''
            self.symbol_data[s] = pd.read_pickle(os.path.join(self.csv_dir, "{}.pkl".format(s)))

            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            self.latest_symbol_data[s] = []

        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index, method='pad').iterrows()


    def _get_new_bar(self, symbol):
        for b in self.symbol_data[symbol]:
            #   print (b[0])
            # yield tuple([symbol, datetime.datetime.strptime(b[0], '%Y-%m-%d %H:%M:%S'), b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])
            yield tuple([symbol, b[0], b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])

    def get_latest_bars(self, symbol, N=1):
        try:
            bars_list = self.latest_symbol_data[symbol]
        except:
            print ("That symbol is not available in the historical data set.")
        else:
            return bars_list[-N:]

    def update_bars(self):
        for s in self.symbol_list:
            try:
                # bar = self._get_new_bar(s).next()  # python 2
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)

        self.events.put(MarketEvent())