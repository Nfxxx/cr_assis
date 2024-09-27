import  os
import pandas as pd
import ccxt, requests

class UpdateData(object):
    def __init__(self):
        self.path = os.environ["HOME"] + "/parameters/config_buffet/dt"
        self.contractsize = pd.read_csv(f"{self.path}/contractsize.csv", index_col = 0)
    
    def get_okex_contractsize(self):
        contractsize = self.contractsize
        exchange = ccxt.okex()
        markets = exchange.load_markets()
        symbols = markets.keys()
        for symbol in symbols:
            if ":" in symbol and "-" not in symbol:
                if "/USDT:USDT" == symbol[-10:]:
                    col = "okex-usdt-swap"
                elif "/USD:" in symbol and symbol.split("/")[0] == symbol.split(":")[-1]:
                    col = "okex-usd-swap"
                else:
                    print(symbol)
                    continue
                coin = symbol.split("/")[0]
                info = markets[symbol]
                contractsize.loc[coin, col] = float(info["contractSize"])
        return contractsize
    
    def get_okexFuture_contractsize(self, suffix: str):
        contractsize = self.contractsize
        exchange = ccxt.okex()
        markets = exchange.load_markets()
        symbols = markets.keys()
        for symbol in symbols:
            if ":" in symbol and "-" in symbol and suffix == symbol.split("-")[-1]:
                if "USDT:USDT" == symbol.split("-")[0].split("/")[-1]:
                    col = "okex-usdt-future"
                elif "/USD:" in symbol and "USD" == symbol.split("-")[0].split("/")[-1].split(":")[0]:
                    col = "okex-usd-future"
                else:
                    print(symbol)
                    continue
                coin = symbol.split("/")[0]
                info = markets[symbol]
                contractsize.loc[coin, col] = float(info["contractSize"])
        return contractsize

    def get_binance_contractsize(self):
        contractsize = self.contractsize
        exchange = ccxt.binanceusdm()
        markets = exchange.load_markets()
        symbols = markets.keys()
        for symbol in symbols:
            if "/USDT" in symbol:
                col = "binance-usdt-swap"
            elif "/BUSD" in symbol:
                col = "binance-busd-swap"
            else:
                continue
            coin = symbol.split("/")[0]
            contractsize.loc[coin, col] = float(markets[symbol]["contractSize"])
        exchange = ccxt.binancecoinm()
        markets = exchange.load_markets()
        symbols = markets.keys()
        for symbol in symbols:
            if "/" in symbol:
                col = "binance-usd-swap"
                coin = symbol.split("/")[0]
                contractsize.loc[coin, col] = float(markets[symbol]["contractSize"])
        return contractsize

    def get_gate_contractsize(self):
        contractsize = self.contractsize
        host = "https://api.gateio.ws"
        prefix = "/api/v4"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        for kind in ["usdt", "usd"]:
            if kind == "usd":
                url = f'/futures/btc/contracts'
            else:
                url = f'/futures/{kind}/contracts'
            col = f"gate-{kind}-swap"
            r = requests.request('GET', host + prefix + url, headers=headers)
            data = r.json()
            for i in range(len(data)):
                info = data[i]
                coin = info["name"].split("_")[0].upper()
                contractsize.loc[coin, col] = float(info["quanto_multiplier"])
                if coin == "BTC":
                    contractsize.loc[coin, col] = 1
        return contractsize

    def get_kucoin_contractsize(self):
        contractsize = self.contractsize
        exchange = ccxt.kucoinfutures()
        markets = exchange.load_markets()
        symbols = markets.keys()
        for symbol in symbols:
            if ":" in symbol and "-" not in symbol:
                if "/USDT:USDT" == symbol[-10:]:
                    col = "kucoin-usdt-swap"
                elif "/USD:" in symbol and symbol.split("/")[0] == symbol.split(":")[-1]:
                    col = "kucoin-usd-swap"
                else:
                    print(symbol)
                    continue
                coin = symbol.split("/")[0]
                info = markets[symbol]
                contractsize.loc[coin, col] = float(info["contractSize"])
        return contractsize

    def update_contractsize(self, path = None):
        if path == None:
            path = self.path
        contractsize = self.get_okex_contractsize()
        contractsize = self.get_binance_contractsize()
        contractsize = self.get_gate_contractsize()
        contractsize = self.get_kucoin_contractsize()
        self.contractsize = contractsize.copy()
        return contractsize
    
    def get_okex_coins(self):
        exchange = ccxt.okex()
        markets = exchange.load_markets()
        existing = pd.DataFrame(columns = ["spot", "usdt-swap", "usd-swap"])
        for key in markets.keys():
            coin, suffix = key.split("/")
            if suffix == "USDT":
                existing.loc[coin, "spot"] = True
            elif suffix == "USDT:USDT":
                existing.loc[coin, "usdt-swap"] = True
            elif suffix == f"USD:{coin}":
                existing.loc[coin, "usd-swap"] = True
        existing.fillna(False, inplace= True)
        return existing
    
    def get_binance_coins(self):
        existing = pd.DataFrame(columns = ["spot", "usdt-swap", "busd-swap","usd-swap"])
        ccxt_columns = {"binanceus": ["spot"], "binanceusdm" : ["usdt-swap", "busd-swap"], "binancecoinm": ["usd-swap"]}
        columns_suffix = {"spot": "USDT", "usdt-swap": "USDT", "busd-swap": "BUSD", "usd-swap": "USD"}
        for key in ccxt_columns.keys():
            exchange = eval(f"ccxt.{key}()")
            markets = exchange.load_markets()
            cols = ccxt_columns[key]
            for col in cols:
                suffix = columns_suffix[col]
                for symbol in markets.keys():
                    if "/" in symbol:
                        coin, x = symbol.split("/")
                        if x == suffix:
                            existing.loc[coin, col] = True
        existing.fillna(False, inplace= True)
        return existing
    
    def get_gate_coins(self):
        existing = pd.DataFrame(columns = ["spot", "usdt-swap"])
        exchange = ccxt.gateio()
        markets = exchange.load_markets()
        symbols = markets.keys()
        for symbol in symbols:
            if "/" in symbol:
                coin, x = symbol.split("/")
                if x == "USDT":
                    existing.loc[coin, "spot"] = True
        host = "https://api.gateio.ws"
        prefix = "/api/v4"
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

        url = f'/futures/usdt/contracts'
        r = requests.request('GET', host + prefix + url, headers=headers)
        datas = r.json()
        for data in datas:
            name = data["name"]
            coin = name.split("_")[0]
            existing.loc[coin, "usdt-swap"] = True
        existing.fillna(False, inplace= True)
        return existing
    
    def get_exchanges_coins(self):
        existing = {}
        exchanges = ["okex", "binance", "gate"]
        for exchange in exchanges:
            data = eval(f"self.get_{exchange}_coins()")
            for col in data.columns:
                existing[f"{exchange}-{col}"] = list(data[data[col]].index.values)
        return existing
    def get_okex_tickSz(self):
        exchange = ccxt.okex()
        markets = exchange.load_markets()
        symbols = markets.keys()
        tickSz = pd.DataFrame(columns = ["spot", "usdt-swap", "usd-swap"])
        for key in markets.keys():
            coin, suffix = key.split("/")
            tick = eval(markets[key]["info"]["tickSz"])
            if suffix == "USDT":
                tickSz.loc[coin, "spot"] = tick
            elif suffix == "USDT:USDT":
                tickSz.loc[coin, "usdt-swap"] = tick
            elif suffix == f"USD:{coin}":
                tickSz.loc[coin, "usd-swap"] = tick
        return tickSz
        
    def get_binance_tickSz(self):
        tickSz = pd.DataFrame(columns = ["spot", "usdt-swap", "busd-swap","usd-swap"])
        ccxt_columns = {"binanceus": ["spot"], "binanceusdm" : ["usdt-swap", "busd-swap"], "binancecoinm": ["usd-swap"]}
        columns_suffix = {"spot": "USDT", "usdt-swap": "USDT", "busd-swap": "BUSD", "usd-swap": "USD"}
        for key in ccxt_columns.keys():
            exchange = eval(f"ccxt.{key}()")
            markets = exchange.load_markets()
            cols = ccxt_columns[key]
            for col in cols:
                suffix = columns_suffix[col]
                for symbol in markets.keys():
                    if "/" in symbol:
                        coin, x = symbol.split("/")
                        if x == suffix:
                            tickSz.loc[coin, col] = eval(markets[symbol]["info"]['filters'][0]["tickSize"])
        return tickSz