from cr_assis.api.gate.client import Client
import requests
import cr_assis.api.gate.consts as c


class AccountAPI(Client):
    
    def __init__(self) -> None:
        super().__init__()
    
    def get_futures_usdt_balance(self) -> requests.Response:
        return self._requests(c.PREFIX + c.FUTURES_USDT_INFO)
    
    def get_futures_usdt_mytrades(self, params = {}) -> requests.Response:
        
        return self._requests(c.PREFIX + c.FUTURES_USDT_TRADE)