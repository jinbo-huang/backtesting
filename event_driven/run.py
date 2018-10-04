# Declare the components with respective parameters
import numpy as np
import pandas as pd
import time

from queue import Queue
from data import HistoricPDDataHandler
from strategy import BuyAndHoldStrategy, MeanReversionStrategy
from execution import SimulatedExecutionHandler
from portfolio import NaivePortfolio

events = Queue()

bars = HistoricPDDataHandler(events, "../data", ["fb"])
# strategy = BuyAndHoldStrategy(bars, events)
strategy = MeanReversionStrategy(bars, events, short_window=5)
# def __init__(self, bars, events, start_date, initial_capital=100000.0):
port = NaivePortfolio(bars, events, "2018-10-3", 100000)
broker = SimulatedExecutionHandler(events)

print ("Days: 50, Strategy: Mean-Reversion, Stock: FB")
print ("Press Enter to Start...")
input()

while bars.continue_backtest == True:

    # print ("Updating Bars.")
    bars.update_bars()

    while True:
        if events.empty():
            break
        else:
            event = events.get(False)
            if event is not None:
                if event.type == 'MARKET':
                    strategy.calculate_signals(event)
                    port.update_timeindex(event)

                elif event.type == 'SIGNAL':
                    port.update_signal(event)

                elif event.type == 'ORDER':
                    broker.execute_order(event)

                elif event.type == 'FILL':
                    port.update_fill(event)

    time.sleep(0.1)

result_stats = port.output_summary_stats()
print ('==========================================')
print (result_stats)