import datetime
import os
import os.path
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
                names=['datetime', 'open', 'low',
                       'high', 'close', 'volume', 'close']
            )

            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            self.latest_symbol_data[s] = []

        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(
                index=comb_index, method='pad').iterrows()

    def _get_new_bar(self, symbol):
        for b in self.symbol_data[symbol]:
            yield tuple([symbol, datetime.datetime.strptime(b[0], '%Y-%m-%d %H:%M:%S'), b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])

    def get_latest_bars(self, symbol, N=1):
        try:
            bars_list = self.latest_symbol_data[symbol]
        except:
            print("That symbol is not available in the historical data set.")
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
            self.symbol_data[s] = pd.read_pickle(os.path.join(
                self.csv_dir, "{}.pkl".format(s))).iloc[0:50]
            # print (self.symbol_data[s].iloc[:10])
            # input()

            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            self.latest_symbol_data[s] = pd.DataFrame(
                columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])

        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(
                index=comb_index, method='pad').iterrows()

    def _get_new_bar(self, symbol):
        for b in self.symbol_data[symbol]:
            #   print (b[0])
            # yield tuple([symbol, datetime.datetime.strptime(b[0], '%Y-%m-%d %H:%M:%S'), b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])
            # yield tuple([symbol, b[0], b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])
            # print (b[0])
            # print (b[1])
            # input()

            row_dict = {}
            row_dict['datetime'] = b[0]
            row_dict['open'] = b[1]['Open']
            row_dict['high'] = b[1]['High']
            row_dict['low'] = b[1]['Low']
            row_dict['close'] = b[1]['Close']
            row_dict['volume'] = b[1]['Volume']
            yield row_dict

            # yield [b[0], b[1]['Open'], b[1]['High'], b[1]['Low'], b[1]['Close'], b[1]['Volume']]

    def get_latest_bars(self, symbol, N=1):
        assert symbol in self.symbol_list, "That symbol is not available in the historical data set."
        return self.latest_symbol_data[symbol][-N:]

    def update_bars(self):
        for s in self.symbol_list:
            try:
                # bar = self._get_new_bar(s).next()  # python 2
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    # print (bar)
                    # input()
                    self.latest_symbol_data[s] = self.latest_symbol_data[s].append(
                        bar, ignore_index=True)
                    # print (self.latest_symbol_data[s])
                    # input()

        self.events.put(MarketEvent())

    def bars_length(self, symbol):
        return len(self.latest_symbol_data[symbol])
