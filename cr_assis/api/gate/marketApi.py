from cr_assis.api.gate.client import Client
import requests
import cr_assis.api.gate.consts as c


class MarketAPI(Client):
    
    def __init__(self) -> None:
        super().__init__()
    
    def get_spot_tickers(self) -> requests.Response:
        return self._requests(c.PREFIX + c.SPOT_TICKERS)