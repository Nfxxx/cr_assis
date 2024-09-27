from .client import Client
from .consts import *

class TradeAPI(Client):

    def __init__(self):
        super().__init__()
        self.name: str = ""
    
    # Place Order
    def place_order(self, instId, tdMode, side, ordType, sz, ccy=None, clOrdId=None, tag=None, posSide=None, px=None,
                    reduceOnly=None, tgtCcy=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        # params = {'instId': instId, 'tdMode': tdMode, 'side': side, 'ordType': ordType, 'sz': sz, 'ccy': ccy,
        #           'clOrdId': clOrdId, 'tag': tag, 'posSide': posSide, 'px': px, 'reduceOnly': reduceOnly}
        return self._requests(PLACR_ORDER, params, method=POST)

    # Place Multiple Orders
    def place_multiple_orders(self, orders_data):
        return self._requests(BATCH_ORDERS, orders_data, method=POST)

    # Cancel Order
    def cancel_order(self, instId, ordId=None, clOrdId=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(CANAEL_ORDER, params, method=POST)

    # Cancel Multiple Orders
    def cancel_multiple_orders(self, orders_data):
        return self._requests(CANAEL_BATCH_ORDERS, orders_data, method=POST)

    # Amend Order
    def amend_order(self, instId, cxlOnFail=None, ordId=None, clOrdId=None, reqId=None, newSz=None, newPx=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(AMEND_ORDER, params, method=POST)

    # Amend Multiple Orders
    def amend_multiple_orders(self, orders_data):
        return self._requests(AMEND_BATCH_ORDER, orders_data, method=POST)

    # Close Positions
    def close_positions(self, instId, mgnMode, posSide=None, ccy=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(CLOSE_POSITION, params, method=POST)

    # Get Order Details
    def get_orders(self, instId, ordId=None, clOrdId=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(ORDER_INFO, params, method=POST)

    # Get Order List
    def get_order_list(self, instType=None, uly=None, instId=None, ordType=None, state=None, after=None, before=None, limit=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(ORDERS_PENDING, params)

    # Get Order History (last 7 days）
    def get_orders_history(self, instType, uly=None, instId=None, ordType=None, state=None, after=None, before=None, limit=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(ORDERS_HISTORY, params)

    # Get Order History (last 3 months)
    def orders_history_archive(self, instType, uly=None, instId=None, ordType=None, state=None, after=None, before=None, begin=None, end=None, limit=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(ORDERS_HISTORY_ARCHIVE, params)

    # Get Transaction Details
    def get_fills(self, instType=None, uly=None, instId=None, ordId=None, after=None, before=None, limit=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(ORDER_FILLS, params)

    # Place Algo Order
    def place_algo_order(self, instId, tdMode, side, ordType, sz, ccy=None, posSide=None, reduceOnly=None, tpTriggerPx=None,
                         tpOrdPx=None, slTriggerPx=None, slOrdPx=None, triggerPx=None, orderPx=None):
        params = {'instId': instId, 'tdMode': tdMode, 'side': side, 'ordType': ordType, 'sz': sz, 'ccy': ccy,
                  'posSide': posSide, 'reduceOnly': reduceOnly, 'tpTriggerPx': tpTriggerPx, 'tpOrdPx': tpOrdPx,
                  'slTriggerPx': slTriggerPx, 'slOrdPx': slOrdPx, 'triggerPx': triggerPx, 'orderPx': orderPx}
        return self._requests(PLACE_ALGO_ORDER, params, method=POST)

    # Cancel Algo Order
    def cancel_algo_order(self, params):
        return self._requests(CANCEL_ALGOS, params, method=POST)

    # Get Algo Order List
    def order_algos_list(self, ordType, algoId=None, instType=None, instId=None, after=None, before=None, limit=None):
        params = {'ordType': ordType, 'algoId': algoId, 'instType': instType, 'instId': instId, 'after': after,
                  'before': before, 'limit': limit}
        return self._requests(ORDERS_ALGO_OENDING, params)

    # Get Algo Order History
    def order_algos_history(self, ordType, state=None, algoId=None, instType=None, instId=None, after=None, before=None, limit=None):
        params = {'ordType': ordType, 'state': state, 'algoId': algoId, 'instType': instType, 'instId': instId,
                  'after': after, 'before': before, 'limit': limit}
        return self._requests(ORDERS_ALGO_HISTORY, params)
