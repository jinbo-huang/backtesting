import datetime
import time
# import ibapi
from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.opt import ibConnection, message

# from IBApi import EClientSocket

from event import FillEvent, OrderEvent
from execution import ExecutionHandler


class IBExecutionHandler(ExecutionHandler):
    def __init__(self, events, order_routing="SMART", currency="USD"):
        self.events = events
        self.order_routing = order_routing
        self.currency = currency
        self.fill_dict = {}

        self.tws_conn = self.create_tws_connection()
        self.order_id = self.create_initial_order_id()
        self.register_handlers()

    def _error_handler(self, msg):
        print("Server Error: {}".format(msg))

    def _reply_handler(self, msg):
        if msg.typeName == "openOrder" and msg.orderId == self.order_id and not msg.orderId in self.fill_dict:
            self.create_fill_dict_entry(msg)
        if msg.typeName == "orderStatus" and msg.status == "Filled" and self.fill_dict[msg.orderId]["filled"] == False:
            self.create_fill(msg)
        print("Server Response: {}, {}".format(msg.typeName, msg))

    def create_tws_connection(self):
        tws_conn = ibConnection()
        tws_conn.connect()
        return tws_conn

    def create_initial_order_id(self):
        return 1

    def register_handlers(self):
        self.tws_conn.register(self._error_handler, 'Error')
        self.tws_conn.registerAll(self._reply_handler)

    def create_contract(self, symbol, sec_type, exch, prim_exch, curr):
        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = sec_type
        contract.m_exchange = exch
        contract.m_primaryExch = prim_exch
        contract.m_currency = curr
        return contract

    def create_order(self, order_type, quantity, action):
        order = Order()
        order.m_orderType = order_type
        order.m_totalQuantity = quantity
        order.m_action = action
        return order

    def create_fill_dict_entry(self, msg):
        self.fill_dict[msg.orderId] = {
            "symbol": msg.contract.m_symbol,
            "exchange": msg.contract.m_exchange,
            "direction": msg.order.m_action,
            "filled": False
        }

    def create_fill(self, msg):
        fd = self.fill_dict[msg.orderId]

        # Prepare the fill data
        symbol = fd["symbol"]
        exchange = fd["exchange"]
        filled = msg.filled
        direction = fd["direction"]
        fill_cost = msg.avgFillPrice

        # Create a fill event object
        fill_event = FillEvent(
            datetime.datetime.utcnow(), symbol,
            exchange, filled, direction, fill_cost
        )

        # Make sure that multiple messages don't create
        # additional fills.
        self.fill_dict[msg.orderId]["filled"] = True

        # Place the fill event onto the event queue
        self.events.put(fill_event)

    def execute_order(self, event):
        if event.type == 'ORDER':
            # Prepare the parameters for the asset order
            asset = event.symbol
            asset_type = "STK"
            order_type = event.order_type
            quantity = event.quantity
            direction = event.direction

            # Create the Interactive Brokers contract via the
            # passed Order event
            ib_contract = self.create_contract(
                asset, asset_type, self.order_routing,
                self.order_routing, self.currency
            )

            # Create the Interactive Brokers order via the
            # passed Order event
            ib_order = self.create_order(
                order_type, quantity, direction
            )

            # Use the connection to the send the order to IB
            self.tws_conn.placeOrder(
                self.order_id, ib_contract, ib_order
            )

            # NOTE: This following line is crucial.
            # It ensures the order goes through!
            time.sleep(1)

            # Increment the order ID for this session
            self.order_id += 1
