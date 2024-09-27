from cr_assis.account.accountOkex import AccountOkex
from cr_assis.account.accountBase import AccountBase
from cr_assis.connect.connectData import ConnectData
from pathlib import Path
import pandas as pd
import numpy as np
import ccxt, copy, requests

class AccountBinance(AccountOkex):
    
    def __init__(self,deploy_id):
        
        super().__init__(deploy_id)
        
        self.exchange_position = "binance"
        self.exchange_combo = "binance"
        self.exchange_contract = "binance"
        self.folder = "pt"
        self.ccy = "USDT"
        self.principal_currency = "USDT"
        self.parameter: pd.DataFrame
        
        self.empty_position:pd.DataFrame = pd.DataFrame(columns = ["usdt", "usdt-swap", "usdt-future", "usd-swap", "usd-future", "busd-swap", "diff", "diff_U"])
        self.open_price: pd.DataFrame = pd.DataFrame(columns = ["usdt", "usdt-swap", "usdt-future", "usd-swap", "usd-future", "busd-swap"])
        self.now_price: pd.DataFrame = pd.DataFrame(columns = ["usdt", "usdt-swap", "usdt-future", "usd-swap", "usd-future", "busd-swap"])
        
           
        self.markets = ccxt.binance().load_markets()
        self.contractsize_uswap = {}
        self.cashBal = {}
        self.contractsize_cswap = {}
        self.exposure_number = 1
        
        self.is_master = {"usd-future":0, "usd-swap":1, "busd-swap":2, "usdt":3,"usdt-future":4, "usdt-swap":5,  "": np.inf}

        if 'portfolio' in deploy_id:
            self.secret_id = {"usd-future": "@binance:futures_usd", "usd-swap": "@binance:swap_usd", "busd-swap": "@binance:swap_usdt",
                            "usdt": "@binance:margin", "usdt-future": "@binance:futures_usdt", "usdt-swap": "@binance:swap_usdt", "": ""}
        else:
            self.secret_id = {"usd-future": "@binance:futures_usd", "usd-swap": "@binance:swap_usd", "busd-swap": "@binance:swap_usdt",
                            "usdt": "@binance:spot", "usdt-future": "@binance:futures_usdt", "usdt-swap": "@binance:swap_usdt", "": ""}
        
              
        self.exchange_master, self.exchange_slave = "binance", "binance"
        self.path_orders = [f'{self.client}_{self.username}@binance_swap_usd', f'{self.client}_{self.username}@binance_swap_usdt']
        self.path_ledgers = [f'{self.client}_{self.username}@binance_swap_usd', f'{self.client}_{self.username}@binance_swap_usdt']
        
    def tell_exposure(self) -> pd.DataFrame:
        data = self.now_position.copy() if hasattr(self, "now_position") else self.empty_position.copy()
        for coin in data.index:
            contractsize = self.contractsize_uswap[coin] if coin in self.contractsize_uswap.keys() else self.get_contractsize_uswap(coin)
            array = data.loc[coin].sort_values()
            array.drop(["diff", "diff_U"], inplace = True)
            array.drop(["is_exposure"], inplace = True) if "is_exposure" in array.index else None
            tell1 = np.isnan(data.loc[coin, "diff"])
            tell2 = np.abs(data.loc[coin, "diff_U"]) >10000                                                               # 单币所有业务敞口之和大于10000u
            tell3 = np.abs((array[0] + array[-1])) * self.get_coin_price(coin) >10000                                     # 单币的敞口大于10000u
            data.loc[coin, "is_exposure"] = tell1 or tell2 or tell3
        data = pd.DataFrame(columns = list(self.empty_position.columns) + ["is_exposure"]) if len(data) == 0 else data
        return data
    
    
    def tell_master(self):
        data = self.master_array.copy()
        data = data.sort_values()
        coin = data.name
        price=self.get_coin_price(coin)
        # 只要有一个条件不满足，认为账户没有这个币
        tell1 = abs(data[0] + data[-1])*price <=1000  and data[0] * data[-1] < 0       
        tell2 = abs(data[1] + data[-1])*price >= 10  or data[1] * data[-1] > 0        
        tell3 = abs(data[0] + data[-2])*price >= 10  or data[0] * data[-2] > 0          
        result = [data.index[0], data.index[-1]] if tell1 and tell2 and tell3 else ["", ""]
        # print(coin,tell1,tell2,tell3)
        ret = {"master": result[0] if self.is_master[result[0]] < self.is_master[result[1]] else result[1],
                "slave": result[0] if self.is_master[result[0]] >= self.is_master[result[1]] else result[1]}
        return ret
    
    def get_api_tickers(self, url: str) -> list:
        response = requests.get(url)
        ret = response.json() if response.status_code == 200 else []
        return ret
    
    def get_tickers(self, instType = "SPOT") -> dict:
        """get market last tickers from binance api
        Args:
            instType (str, optional): It should be in SPOT, SWAP, FUTURES or OPTION. Defaults to "SPOT".
        """
        instType = instType.upper()
        if instType == "SPOT":
            ret = self.get_api_tickers("https://api.binance.com/api/v3/ticker/24hr")
        elif instType in ["SWAP", "FUTURES"]:
            ret = self.get_api_tickers("https://fapi.binance.com/fapi/v1/ticker/24hr") + self.get_api_tickers("https://dapi.binance.com/dapi/v1/ticker/24hr")
        else:
            print(f"instType {instType} is not support in AccountBinance")
            ret = []
        data = {i["symbol"]: i for i in ret}
        self.tickers[instType] = data
        return data
    
    
    def get_coin_price(self, coin: str) -> float:
        self.get_tickers() if "SPOT" not in self.tickers.keys() else None
        ret = float(self.tickers["SPOT"][f"{coin.upper()}USDT"]["lastPrice"]) if f"{coin.upper()}USDT" in self.tickers["SPOT"].keys() else np.nan
        return ret