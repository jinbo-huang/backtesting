# Declare the components with respective parameters
import numpy as np
import pandas as pd
import time

from queue import Queue
from data import HistoricPDDataHandler
from strategy import BuyAndHoldStrategy
from execution import SimulatedExecutionHandler
from portfolio import NaivePortfolio

events = Queue()

bars = HistoricPDDataHandler(events, "../data", ["fb"])
strategy = BuyAndHoldStrategy(bars, events)
# def __init__(self, bars, events, start_date, initial_capital=100000.0):
port = NaivePortfolio(bars, events, "2018-10-3", 100000)
broker = SimulatedExecutionHandler(events)

while bars.continue_backtest == True:

    print ("Updating Bars.")
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

    time.sleep(1)

'''
while True:
    # Update the bars (specific backtest code, as opposed to live trading)
    if bars.continue_backtest == True:
        bars.update_bars()
    else:
        break
    
    # Handle the events
    while True:
        try:
            event = events.get(False)
        except Queue.Empty:
            break
        else:
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

    # 10-Minute heartbeat
    time.sleep(10*60)
'''