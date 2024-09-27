from cr_assis.api.okex.client import Client
import cr_assis.api.okex.consts as c

class MarketAPI(Client):
    
    def __init__(self) -> None:
        super().__init__()
    
    def get_tickers(self, instType, uly=None):
        params = {'instType': instType, 'uly': uly} if uly else {'instType': instType}
        return self._requests(query = c.TICKERS_INFO, params = params)

    def get_ticker(self, instId):
        params = {'instId': instId}
        return self._requests(c.TICKER_INFO, params)

    def get_index_ticker(self, quoteCcy=None, instId=None):
        params = {'quoteCcy': quoteCcy, 'instId': instId}
        return self._requests(c.INDEX_TICKERS, params)

    def get_orderbook(self, instId, sz=None):
        params = {'instId': instId, 'sz': sz} if sz else {'instId': instId} 
        return self._requests(c.ORDER_BOOKS, params)

    def get_candlesticks(self, instId, after=None, before=None, bar=None, limit=None):
        params = {'instId': instId, 'after': after, 'before': before, 'bar': bar, 'limit': limit}
        return self._requests(c.MARKET_CANDLES, params)

    def get_history_candlesticks(self, instId, after=None, before=None, bar=None, limit=None):
        params = {'instId': instId, 'after': after, 'before': before, 'bar': bar, 'limit': limit}
        return self._requests(c.HISTORY_CANDLES, params)

    def get_index_candlesticks(self, instId, after=None, before=None, bar=None, limit=None):
        params = {'instId': instId, 'after': after, 'before': before, 'bar': bar, 'limit': limit}
        return self._requests(c.INDEX_CANSLES, params)

    def get_markprice_candlesticks(self, instId, after=None, before=None, bar=None, limit=None):
        params = {'instId': instId, 'after': after, 'before': before, 'bar': bar, 'limit': limit}
        return self._requests(c.MARKPRICE_CANDLES, params)

    def get_trades(self, instId, limit=None):
        params = {'instId': instId, 'limit': limit}
        return self._requests(c.MARKET_TRADES, params)

    def get_volume(self):
        return self._requests(c.VOLUMNE)

    def get_oracle(self):
        return self._requests(c.ORACLE)

    def get_tier(self, instType=None, tdMode=None, uly=None, instId=None, ccy=None, tier=None):
        params = {'instType': instType, 'tdMode': tdMode, 'uly': uly, 'instId': instId, 'ccy': ccy, 'tier': tier}
        return self._requests(c.TIER, params)
    
    def get_lending_history(self, ccy = None, after = None, before = None, limit = None):
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        return self._requests(c.LENDING_RATE_HISTORY, params)
    
    def get_lending_summary(self):
        return self._requests(c.LENDING_RATE_SUMMARY)