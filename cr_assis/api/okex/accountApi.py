from cr_assis.api.okex.client import Client
import cr_assis.api.okex.consts as c

class AccountAPI(Client):

    def __init__(self) -> None:
        super().__init__()
        self.name: str = ""

    def get_position_risk(self, instType=None):
        params = {'instType': instType}
        return self._requests(c.POSITION_RISK, params)

    def get_account_balance(self, ccy: str=None):
        params = {'ccy': ccy}
        return self._requests(c.ACCOUNT_INFO, params)

    def get_positions(self, instType=None, instId=None):
        params = {'instType': instType, 'instId': instId}
        return self._requests(c.POSITION_INFO, params)

    def get_bills_detail(self, instType=None, ccy=None, mgnMode=None, ctType=None, type=None, subType=None, after=None, before=None,
                        limit=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(c.BILLS_DETAIL, params)

    def get_bills_details(self, instType=None, ccy=None, mgnMode=None, ctType=None, type=None, subType=None, after=None, before=None, begin=None, end = None,
                        limit=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(c.BILLS_ARCHIVE, params)
    
    def get_order(self, instId:str, ordId=None, clOrdId=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(c.ORDER_INFO, params)
    
    def get_order_history(self, instType: str, uly= None, instFamily=None, instId=None, ordType=None, state=None, category=None, after=None, before=None, begin=None, end=None, limit=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(c.ORDERS_HISTORY, params)
    
    def get_account_config(self):
        return self._requests(c.ACCOUNT_CONFIG)

    def get_position_mode(self, posMode):
        params = {'posMode': posMode}
        return self._requests(c.POSITION_MODE, params)

    def set_leverage(self, lever, mgnMode, instId=None, ccy=None, posSide=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(c.SET_LEVERAGE, params, method=c.POST)

    def get_maximum_trade_size(self, instId, tdMode, ccy=None, px=None):
        params = {'instId': instId, 'tdMode': tdMode, 'ccy': ccy, 'px': px}
        return self._requests(c.MAX_TRADE_SIZE, params)

    def get_max_avail_size(self, instId, tdMode, ccy=None, reduceOnly=None):
        params = {'instId': instId, 'tdMode': tdMode, 'ccy': ccy, 'reduceOnly': reduceOnly}
        return self._requests(c.MAX_AVAIL_SIZE, params)

    def Adjustment_margin(self, instId, posSide, type, amt):
        params = {'instId': instId, 'posSide': posSide, 'type': type, 'amt': amt}
        return self._requests(c.ADJUSTMENT_MARGIN, params, method=c.POST)

    def get_leverage(self, instId, mgnMode):
        params = {'instId': instId, 'mgnMode': mgnMode}
        return self._requests(c.GET_LEVERAGE, params)

    def get_max_loan(self, instId, mgnMode, mgnCcy=None):
        params = {'instId': instId, 'mgnMode': mgnMode, 'mgnCcy': mgnCcy}
        return self._requests(c.MAX_LOAN, params)

    def get_fee_rates(self, instType, instId=None, uly=None, category=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(c.FEE_RATES, params)

    def get_interest_accrued(self, instId=None, ccy=None, mgnMode=None, after=None, before=None, limit=None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(c.INTEREST_ACCRUED, params)

    def get_interest_rate(self, ccy=None):
        params = {'ccy': ccy}
        return self._requests(c.INTEREST_RATE, params)

    def set_greeks(self, greeksType):
        params = {'greeksType': greeksType}
        return self._requests(c.SET_GREEKS, params, method=c.POST)

    def get_max_withdrawal(self, ccy=None):
        params = {'ccy': ccy}
        return self._requests(c.MAX_WITHDRAWAL, params)
