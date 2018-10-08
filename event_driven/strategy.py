import datetime
import numpy as np
import pandas as pd

from queue import Queue
from abc import ABCMeta, abstractmethod

from event import SignalEvent


class Strategy(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def calculate_signals(self):
        raise NotImplementedError("Should implement calculate_signals()")


class BuyAndHoldStrategy(Strategy):
    def __init__(self, bars, events):
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events

        self.bought = self._calculate_initial_bought()

    def _calculate_initial_bought(self):
        bought = {}
        for s in self.symbol_list:
            bought[s] = False
        return bought

    def calculate_signals(self, event):
        # if event.type == 'MARKET':
        assert event.type == 'MARKET', "The event type should be MARKET"
        for s in self.symbol_list:
            bars = self.bars.get_latest_bars(s, N=1)
            if bars is not None and bars != []:
                if self.bought[s] == False:
                    signal = SignalEvent(bars[0][0], bars[0][1], 'LONG')
                    print("Send long signal")
                    self.events.put(signal)
                    self.bought[s] = True


class MeanReversionStrategy(Strategy):
    def __init__(self, bars, events, long_window=100, short_window=40):
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.long_window = long_window
        self.short_window = short_window
        self.status = 0

    def _calculate_initial_bought(self):
        bought = {}
        for s in self.symbol_list:
            bought[s] = False
        return bought

    def calculate_signals(self, event):
        assert event.type == 'MARKET', "The event type should be MARKET"

        '''
        for s in self.symbol_list:
            bars = self.bars.get_latest_bars(s, N=1)
            self.long_mavg[s] = self.long_mavg[s] * self.bars_count + bars[3]
            self.bars_count = self.bars_count + 1
        '''

        for s in self.symbol_list:
            # if self.bars.bars_length(s) > self.short_window and self.bought[s] == False:
            if self.bars.bars_length(s) > self.short_window:
                bars = self.bars.get_latest_bars(s, N=self.long_window)
                short_mavg = bars.iloc[-self.short_window:]['close'].mean()
                long_mavg = bars['close'].mean()
                signal = None
                if short_mavg > long_mavg and self.status is not 1:
                    signal = SignalEvent(s, bars.iloc[-1]['datetime'], 'LONG')
                    print("short mavg: {:.3f}, long mavg: {:.3f} ==> Long".format(
                        short_mavg, long_mavg))
                    self.status = 1
                elif short_mavg < long_mavg and self.status is not -1:
                    signal = SignalEvent(s, bars.iloc[-1]['datetime'], 'SHORT')
                    print("short mavg: {:.3f}, long mavg: {:.3f} ==> Short".format(
                        short_mavg, long_mavg))
                    self.status = -1
                # print ("{} bars length: {}. Testing~~~".format(s, self.bars.bars_length(s)))
                # signal = SignalEvent(bars[0][0], bars[0][1], 'LONG')
                if signal is not None:
                    self.events.put(signal)
                # self.bought[s] = True

        '''
        # Initialize the `signals` DataFrame with the `signal` column
        signals = pd.DataFrame(index=self.bars.index)
        signals['signal'] = 0.0

        # Create short simple moving average over the short window
        signals['short_mavg'] = self.bars['Close'].rolling(window=short_window, min_periods=1, center=False).mean()

        # Create long simple moving average over the long window
        signals['long_mavg'] = self.bars['Close'].rolling(window=long_window, min_periods=1, center=False).mean()

        # Create signals
        signals['signal'][short_window:] = np.where(signals['short_mavg'][short_window:] 
                                                    > signals['long_mavg'][short_window:], 1.0, 0.0)   

        # Generate trading orders
        signals['positions'] = signals['signal'].diff()
        

        # signals['positions']
        return signals
        '''
