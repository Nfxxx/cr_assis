from cr_assis.account.accountBase import AccountBase
from cr_assis.connect.connectData import ConnectData
from cr_assis.connect.connectOkex import ConnectOkex
from cr_assis.api.okex.marketApi import MarketAPI
from cr_assis.eva import eva
from pathlib import Path
import pandas as pd
import numpy as np
import ccxt, copy, datetime, pytz
from cr_assis.draw import draw_ssh
from bokeh.plotting import show
from bokeh.models.widgets import Panel, Tabs

class AccountOkex(AccountBase):
    """Account Information only in Okex
    """
    
    def __init__(self, deploy_id: str) -> None:
        self.deploy_id = deploy_id
        self.balance_id = self.deploy_id.replace("@", "-") + "@sum"
        self.exchange_position = "okexv5"
        self.exchange_combo = "okex"
        self.exchange_contract = "okex"
        self.folder = "pt"
        self.ccy = "BTC"
        self.principal_currency = "BTC"
        self.parameter: pd.DataFrame
        self.empty_position:pd.DataFrame = pd.DataFrame(columns = ["usdt", "usdt-swap", "usdt-future", "usd-swap", "usd-future", "usdc-swap", "diff", "diff_U"])
        self.open_price: pd.DataFrame = pd.DataFrame(columns = ["usdt", "usdt-swap", "usdt-future", "usd-swap", "usd-future", "usdc-swap"])
        self.now_price: pd.DataFrame = pd.DataFrame(columns = ["usdt", "usdt-swap", "usdt-future", "usd-swap", "usd-future", "usdc-swap"])
        self.usd_position: pd.DataFrame = pd.DataFrame(columns = ["usd-swap"])
        self.script_path = str(Path( __file__ ).parent.parent.absolute())
        self.mongon_url = self.load_mongo_url()
        self.parameter_name = deploy_id.split("@")[0]
        self.client, self.username = self.parameter_name.split("_")
        self.database = ConnectData()
        self.dataokex = ConnectOkex()
        self.markets = ccxt.okex().load_markets()
        self.market_api = MarketAPI()
        self.tickers : dict[str, dict]= {}
        self.contractsize_uswap : dict[str, float] = {"BETH": 0.1}
        self.cashBal : dict[str, float] = {}
        self.contractsize_cswap : dict[str, float] = {"BTC": 100, "ETH": 10, "FIL": 10, "LTC": 10, "DOGE": 10, "ETC": 10}
        self.exposure_number = 1
        self.ignore_mv = 0.5
        self.is_master = {"usd-future":0, "usd-swap":1, "bethusdt":1.5, "usdt":2,"usdc-swap":3, "usdt-future":4, "usdt-swap":5,  "": np.inf}
        self.secret_id = {"usd-future": "@okexv5:futures_usd", "usd-swap": "@okexv5:swap_usd", "usdc-swap": "@okexv5:swap_usdt",
                        "usdt": "@okexv5:spot", "bethusdt": "@okexv5:spot", "usdt-future": "@okexv5:futures_usdt", "usdt-swap": "@okexv5:swap_usdt", "": ""}
        self.exchange_master, self.exchange_slave = "okex", "okex"
        self.path_orders = [f'{self.parameter_name}@okexv5_swap_usd', f'{self.parameter_name}@okexv5_swap_usdt', f'{self.parameter_name}@okexv5_spot']
        self.path_ledgers = [f'{self.parameter_name}@okexv5_swap_usd', f'{self.parameter_name}@okexv5_swap_usdt']
        self.end: datetime.datetime = datetime.datetime.now().astimezone(pytz.timezone("Asia/Shanghai")).replace(tzinfo = None)
        self.start: datetime.datetime = self.end + datetime.timedelta(days = -3)
        self.datacenter = "/mnt/efs/fs1/data_center/orders"
    
    def get_contractsize(self, symbol: str) -> float:
        return self.markets[symbol]["contractSize"] if symbol in self.markets.keys() else np.nan
    
    def get_pair_suffix_contract(self, contract: str) -> str:
        """get pair suffix from master or slave
        """
        contract = contract.replace("-", "_").replace("spot", "usdt")
        contract = contract.replace(contract.split("_")[0], "")
        return contract
    
    def get_pair_suffix(self, combo: str, future: str) -> tuple[str, str]:
        """get pair suffix from a combo
        """
        master, slave = combo.split("-")
        master, slave = self.get_pair_suffix_contract(master), self.get_pair_suffix_contract(slave)
        return master.replace("future", future), slave.replace("future", future)
    
    def transfer_beth_swap(self, contract: str) -> str:
        contract = contract.replace("_", "-").lower()
        if contract.split("-")[0].lower() == "beth" and contract.lower() != "beth-usdt":
            contract = contract.replace("beth", "eth")
        return contract
    
    def get_pair_name(self, coin: str, combo: str) -> tuple[str, str]:
        master_suffix, slave_suffix = self.get_pair_suffix(combo, "future")
        return (self.transfer_beth_swap(coin+master_suffix), self.transfer_beth_swap(coin+slave_suffix)) if combo != "okex_spot-okex_spot" or coin.lower() != "beth" else ("beth-usdt", "eth-usdt")
    
    def get_secret_name(self, coin: str, combo: str) -> tuple[str, str]:
        master_suffix, slave_suffix = self.get_pair_suffix(combo, "future")
        return self.parameter_name+self.secret_id[master_suffix[1:].replace("_", "-")], self.parameter_name+self.secret_id[slave_suffix[1:].replace("_", "-")]
        
    def get_spreads(self, coin: str, combo: str, suffix="", start="now() - 24h", end = "now()") -> pd.DataFrame:
        """get spreads data
        Args:
            coin (str): coin name, str.lower
            combo (str): combo name, like "okx_usd_swap-okx_usdt_swap"
            suffix (str, optional): future delivery time. Defaults to "", which means this quarter
            start (str, optional): start time. Defaults to "now() - 24h".
            end (str, optional): end time. Defaults to "now()".

        Returns:
            pd.DataFrame: spreads data, columns = ["time", "ask0_spread", "bid0_spread", "dt"]
        """
        coin = coin.lower()
        master, slave = combo.split("-")
        start, end = self.get_influx_time_str(start), self.get_influx_time_str(end)
        suffix = self.get_quarter() if suffix == "" else suffix
        contract_master, contract_slave = self.get_pair_suffix(combo, future = suffix)
        kind_master = master.split("_")[-1].replace("future", "futures")
        kind_slave = slave.split("_")[-1].replace("future", "futures")
        database = self.get_all_database()
        dataname = f'''spread_orderbook_{self.exchange_contract}_{kind_master}_{coin}{contract_master}__orderbook_{self.exchange_contract}_{kind_slave}_{coin}{contract_slave}'''
        dataname_reverse = f'''spread_orderbook_{self.exchange_contract}_{kind_slave}_{coin}{contract_slave}__orderbook_{self.exchange_contract}_{kind_master}_{coin}{contract_master}'''
        is_exist = True
        if dataname in database:
            a = f"select time, ask0_spread, bid0_spread from {dataname} where time >= {start} and time <= {end}"
        elif dataname_reverse in database:
            a = f"select time, 1/ask0_spread as ask0_spread, 1/bid0_spread as bid0_spread from {dataname_reverse} where time >= {start} and time <= {end}"
        elif "future" in combo:
            if suffix in contract_master:
                dataname = f'''spread_orderbook_{self.exchange_contract}_{kind_master}_{coin}{contract_master}__orderbook_{self.exchange_contract}_spot_{coin}_usdt'''
                if dataname in database:
                    a = f"select time, ask0_spread, bid0_spread from {dataname} where time >= {start} and time <= {end}"
                else:
                    is_exist = False
            else:
                dataname_reverse = f'''spread_orderbook_{self.exchange_contract}_{kind_slave}_{coin}{contract_slave}__orderbook_{self.exchange_contract}_spot_{coin}_usdt'''
                if dataname_reverse in database:
                    a = f"select time, 1/ask0_spread as ask0_spread, 1/bid0_spread as bid0_spread from {dataname_reverse} where time >= {start} and time <= {end}"
                else:
                    is_exist = False
        else:
            is_exist = False
        spreads_data = pd.DataFrame(columns = ["time", "ask0_spread", "bid0_spread", "dt"])
        if not is_exist:
            print(f"{self.parameter_name} {combo} {coin} spreads database doesn't exist")
        else:
            return_data = self.database._send_influx_query(sql = a, database = "spreads", is_dataFrame= True)
            if len(return_data) > 0:
                spreads_data = return_data
                spreads_data["dt"] = spreads_data["time"].apply(lambda x: datetime.datetime.strptime(x[:19],'%Y-%m-%dT%H:%M:%S') + datetime.timedelta(hours = 8))
        return spreads_data
        
    def get_contractsize_cswap(self, coin: str) ->float:
        coin = coin.upper()
        symbol = f"{coin}/USD:{coin}"
        contractsize = self.get_contractsize(symbol)
        self.contractsize_cswap[coin] = contractsize
        return contractsize
    
    def get_contractsize_uswap(self, coin: str) ->float:
        coin = coin.upper()
        symbol = f"{coin}/USDT:USDT"
        contractsize = self.get_contractsize(symbol)
        self.contractsize_uswap[coin] = contractsize
        return contractsize
    
    def get_influx_position(self, timestamp: str, the_time: str) -> pd.DataFrame:
        a = f"""
            select ex_field, secret_id,long, long_open_price, settlement, short, short_open_price, pair from position 
            where client = '{self.client}' and username = '{self.username}' and pair != 'usdc-usdt' and pair != 'busd-usdt' and 
            time > {the_time} - {timestamp} and time < {the_time} and (long >0 or short >0) and secret_id != None
            and exchange = '{self.exchange_position}' group by pair, ex_field, exchange ORDER BY time DESC
            """
        data = self._send_complex_query(sql = a)
        if len(data) == 0:
            a = f"""
            select ex_field, secret_id,long, long_open_price, settlement, short, short_open_price, pair from position 
            where client = '{self.client}' and username = '{self.username}' and pair != 'usdc-usdt' and pair != 'busd-usdt'
            time > {the_time} - {timestamp} and time < {the_time} and secret_id != None
            and exchange = '{self.exchange_position}' group by pair, ex_field, exchange ORDER BY time DESC
            """
            data = self._send_complex_query(sql = a)
        data.dropna(subset = ["secret_id"], inplace= True) if "secret_id" in data.columns else None
        data = data.sort_values(by = "time").drop_duplicates(subset= ["pair", "ex_field", "secret_id"], keep = "last") if len(data) > 0 else data
        return data
    
    def find_future_position(self, coin: str, raw_data: pd.DataFrame, col: str) -> pd.DataFrame:
        data = raw_data[(raw_data["ex_field"] == "futures") & (raw_data["coin"] == coin)].copy()
        data["col"] = data["pair"].apply(lambda x: x.split("-")[1] if type(x) == str else None)
        data = data[data["col"] == col.split("-")[0]].copy()
        data["is_future"] = data["pair"].apply(lambda x: str.isnumeric(x.split("-")[-1]))
        data = data[data["is_future"] == True].copy()
        return data
    
    def gather_future_position(self, coin: str, raw_data: pd.DataFrame, col: str) -> float:
        """Gather different future contracts positions about this coin
        """
        self.future_position: pd.DataFrame = pd.DataFrame()
        data = self.find_future_position(coin, raw_data, col).drop_duplicates(subset = ["pair"], keep= "last")
        if col.split("-")[0] != "usd":
            amount = data["long"].sum() - data["short"].sum()
        else:
            contractsize = self.contractsize_cswap[coin] if coin in self.contractsize_cswap.keys() else self.get_contractsize_cswap(coin)
            amount = ((data["long"] - data["short"]) * contractsize / (data["long_open_price"] + data["short_open_price"])).sum()
        for i in data.index:
            coin = data.loc[i, "coin"]
            pair = data.loc[i, "pair"].replace(coin.lower(), "")[1:]
            self.future_position.loc[coin, pair] = data.loc[i, "long"] - data.loc[i, "short"]
        return amount
    
    def gather_coin_position(self, coin: str, all_data: pd.DataFrame) -> pd.DataFrame:
        """Gather positions of different contracts about this coin

        Args:
            coin (str): coin name, str.upper
            all_data (pd.DataFrame): origin position
        """
        result = self.empty_position.copy()
        coin = coin.upper()
        for col in result.columns:
            if "future" not in col and "diff" not in col:
                data = all_data[all_data["pair"] == f"{coin.lower()}-{col}"].copy()
                result.loc[coin, col] = (data["long"] - data["short"]).values[-1] if len(data) > 0 else 0
                if col.split("-")[0] == "usd" and result.loc[coin, col] != 0:
                    self.usd_position.loc[coin, col] = result.loc[coin, col]
                    contractsize = self.contractsize_cswap[coin] if coin in self.contractsize_cswap.keys() else self.get_contractsize_cswap(coin)
                    open_price = (data["long_open_price"] + data["short_open_price"]).values[-1] if len(data) > 0 else np.nan
                    result.loc[coin, col] = result.loc[coin, col] * contractsize / open_price if open_price != 0 else np.nan
            elif "future" == col.split("-")[-1]:
                result.loc[coin, col] = self.gather_future_position(coin = coin, raw_data = all_data, col = col)
            else:
                pass
        return result
        
    def gather_position(self) -> pd.DataFrame:
        """Gather the positions of different contracts of the same coin
        """
        data = self.origin_position if hasattr(self, "origin_position") and "pair" in self.origin_position.columns else pd.DataFrame(columns = ["pair"])
        data.sort_values(by = "time", inplace= True) if "time" in data.columns else None
        data["coin"] = data["pair"].apply(lambda x: x.split("-")[0].upper() if type(x) == str else "")
        coins = list(data["coin"].unique())
        result = self.empty_position.copy()
        for coin in coins:
            ret = self.gather_coin_position(coin = coin, all_data = data) if coin != "" else self.empty_position
            result = pd.concat([result, ret])
        return result
            
    def calculate_exposure(self):
        data = self.now_position.copy() if hasattr(self, "now_position") else self.empty_position.copy()
        cols = [x for x in self.empty_position.columns if "diff" not in x]
        data["diff"] = data[cols].sum(axis = 1)
        for coin in data.index:
            data.loc[coin, "diff_U"] = data.loc[coin, "diff"] * self.get_coin_price(coin)
        return data
    
    def get_tickers(self, instType = "SPOT") -> dict:
        """get market last tickers from okex api
        Args:
            instType (str, optional): It should be in SPOT, SWAP, FUTURES or OPTION. Defaults to "SPOT".
        """
        response = self.market_api.get_tickers(instType)
        ret = response.json() if response.status_code == 200 else {"data": []}
        data = {i["instId"]: i for i in ret["data"]}
        self.tickers[instType] = data
        return data
    
    def get_coin_price(self, coin: str) -> float:
        self.get_tickers() if "SPOT" not in self.tickers.keys() else None
        ret = float(self.tickers["SPOT"][f"{coin.upper()}-USDT"]["last"]) if f"{coin.upper()}-USDT" in self.tickers["SPOT"].keys() else np.nan
        return ret
    
    def is_ccy_exposure(self) -> bool:
        other_array = self.exposure_array.drop(["usdt", "diff_U", "diff"]).sort_values()
        not_spot = abs(other_array.sum()) > self.exposure_number * self.exposure_contractsize * 5 or abs(other_array[0] + other_array[-1]) > self.exposure_number * self.exposure_contractsize * 2
        is_spot = abs(other_array[0]) >= self.exposure_number * self.exposure_contractsize and abs(other_array[-1]) >= self.exposure_number * self.exposure_contractsize
        return not_spot and is_spot
    
    def is_eth_exposure(self) -> bool:
        tell1 = np.isnan(self.exposure_array["diff"])
        n = 0
        number = []
        for i in ["BETH", "ETH"]:
            n += self.now_position.loc[i, "diff"] if i in self.now_position.index else 0
            number += list(self.now_position.loc[i, self.open_price.columns].values) if i in self.now_position.index else []
        tell2 = abs(n) > self.exposure_number * self.exposure_contractsize * 6
        number.sort()
        tell3 = abs(number[0] + number[-1]) > self.exposure_number * self.exposure_contractsize
        return tell1 or tell2 or tell3
    
    def is_other_exposure(self) -> bool:
        tell1 = np.isnan(self.exposure_array["diff"])
        tell2 = abs(self.exposure_array["diff"]) > self.exposure_number * self.exposure_contractsize * 6
        array = self.exposure_array[list(self.open_price.columns)].sort_values()
        tell3 = abs(array[0] + array[-1]) > self.exposure_number * self.exposure_contractsize
        return tell1 or tell2 or tell3
    
    def tell_coin_exposure(self, coin: str) -> bool:
        coin = coin.upper()
        self.exposure_array = self.now_position.loc[coin]
        self.exposure_contractsize = self.contractsize_uswap[coin] if coin in self.contractsize_uswap.keys() else self.get_contractsize_uswap(coin)
        if coin == self.ccy:
            ret = self.is_ccy_exposure()
        elif coin in ["BETH", "ETH"]:
            ret = self.is_eth_exposure()
        else:
            ret = self.is_other_exposure()
        return ret
    
    def tell_exposure(self) -> pd.DataFrame:
        data = self.now_position.copy() if hasattr(self, "now_position") else self.empty_position.copy()
        for coin in data.index:
            data.loc[coin, "is_exposure"] = self.tell_coin_exposure(coin)
        data = pd.DataFrame(columns = list(self.empty_position.columns) + ["is_exposure"]) if len(data) == 0 else data
        return data
    
    def get_now_position(self, timestamp="10m", the_time="now()") -> pd.DataFrame:
        the_time = f"'{the_time}'" if "now()" not in the_time and "'" not in the_time else the_time
        self.usd_position: pd.DataFrame = pd.DataFrame(columns = ["usd-swap"])
        self.origin_position = self.get_influx_position(timestamp = timestamp, the_time=the_time)
        self.now_position: pd.DataFrame = self.gather_position()
        self.now_position = self.calculate_exposure()
        self.now_position = self.tell_exposure()
        return self.now_position.copy()
    
    def get_open_price(self) -> pd.DataFrame:
        self.get_now_position() if not hasattr(self, "now_position") else None
        for coin in self.now_position.index:
            for contract in self.open_price.columns:
                if self.now_position.loc[coin, contract] != 0:
                    df = self.origin_position[self.origin_position["pair"] == f"{coin.lower()}-{contract}"].copy() if "future" not in contract else self.find_future_position(coin, self.origin_position, contract)
                    self.open_price.loc[coin, contract] = (df["long_open_price"] + df["short_open_price"]).values[-1] if len(df) > 0 else np.nan
                else:
                    self.open_price.loc[coin, contract] = 0
            if self.now_position.loc[coin, "usdt"] != 0:
                max_col = abs(self.now_position.loc[coin, self.open_price.columns.drop("usdt")]).sort_values().index[-1]
                self.open_price.loc[coin, "usdt"] = self.open_price.loc[coin, max_col] if self.open_price.loc[coin, max_col] >0 else self.get_coin_price(coin)
        if "BETH" in self.now_position.index and self.now_position.loc["BETH", "usdt"] > 0 and "ETH" in self.now_position.index:
            max_col = abs(self.now_position.loc["ETH", self.open_price.columns.drop("usdt")]).sort_values().index[-1]
            self.open_price.loc["BETH", "usdt"] = self.open_price.loc["ETH", max_col]
        return self.open_price.copy()
    
    def get_cashBal(self, coin: str) -> float:
        a = f"""
        select last(origin) as origin FROM "equity_snapshot" WHERE time > now() - 10m and username = '{self.username}' and client = '{self.client}' and symbol = '{coin.lower()}'
        """
        ret = self.database._send_influx_query(a, database = "account_data", is_dataFrame= True)
        self.cashBal[coin.upper()] = float(eval(ret["origin"].values[-1])["cashBal"]) if len(ret) > 0 else np.nan
        return self.cashBal[coin.upper()]
    
    def get_now_price(self):
        self.get_now_position() if not hasattr(self, "now_position") else None
        for coin in set(self.now_position.index) | set([self.ccy]):
            self.now_price.loc[coin] = self.get_coin_price(coin)
    
    def tell_ccy_master(self) -> dict[str, str]:
        self.master_array = self.master_array.drop("usdt").sort_values()
        ret = self.tell_master()
        if "" in ret.values() and not self.now_position.loc[self.ccy, "is_exposure"]:
            ret = {"master":self.master_array[abs(self.master_array) == abs(self.master_array).max()].index[0], "slave": "usdt"} if abs(self.master_array).max() > self.exposure_number * self.master_contractsize * 2 else {"master": "", "slave": ""}
            self.master_array = self.now_position.loc[self.ccy].drop(["diff", "diff_U", "is_exposure"])
        return ret
    
    def tell_master(self) -> dict[str, str]:
        data = self.master_array.sort_values()
        contractsize = self.master_contractsize
        tell1 = abs(data[0] + data[-1]) <= contractsize * self.exposure_number and data[0] * data[-1] < 0
        tell2 = abs(data[1] + data[-1]) >= contractsize * self.exposure_number or data[1] * data[-1] > 0
        tell3 = abs(data[0] + data[-2]) >= contractsize * self.exposure_number or data[0] * data[-2] > 0
        result = [data.index[0], data.index[-1]] if tell1 and tell2 and tell3 else ["", ""]
        ret = {"master": result[0] if self.is_master[result[0]] < self.is_master[result[1]] else result[1],
                "slave": result[0] if self.is_master[result[0]] >= self.is_master[result[1]] else result[1]}
        return ret
    
    def tell_eth_master(self) -> dict[str, str]:
        ret = self.tell_master()
        if "" in ret.values() and not self.now_position.loc["ETH", "is_exposure"] and "BETH" in self.now_position.index:
            self.master_array["bethusdt"] = self.now_position.loc["BETH", "usdt"]
            ret = self.tell_master()
            self.execute_coin = "BETH" if "bethusdt" in ret.values() else self.execute_coin
            for k, v in ret.items():
                ret[k] = v.replace("bethusdt", "usdt")
        return ret
    
    def tell_coin_master(self) -> dict[str, str]:
        coin = self.execute_coin.upper()
        self.master_array = self.now_position.loc[coin].drop(["diff", "diff_U", "is_exposure"])
        self.master_contractsize = self.contractsize_uswap[coin] if coin in self.contractsize_uswap.keys() else self.get_contractsize_uswap(coin)
        if coin == self.ccy:
            ret = self.tell_ccy_master()
        elif coin == "ETH":
            ret = self.tell_eth_master()
        else:
            ret = self.tell_master()
        return ret
    
    def transfer_pair(self, pair: str) -> str:
        ret = pair.replace("-", "_")
        ret = ret.replace("usdt", "spot") if ret.split("_")[-1] == "usdt" and "swap" not in ret else ret
        return ret
            
    def get_coin_combo(self, coin: str, master_pair: str, slave_pair: str) -> str:
        master = self.transfer_pair(master_pair.replace(coin.lower(), ""))
        slave = self.transfer_pair(slave_pair.replace(coin.lower(), ""))
        return f"{self.exchange_combo}{master}-{self.exchange_combo}{slave}"
    
    def get_unknown_coin_position(self) -> None:
        position, num, coin, array = self.position, len(self.position), self.execute_coin, self.master_array.sort_values()
        position.loc[num, "coin"] = coin.lower()
        result =[array.index[0], array.index[-1]]
        maybe = {"master": result[0] if self.is_master[result[0]] < self.is_master[result[1]] else result[1],
        "slave": result[0] if self.is_master[result[0]] >= self.is_master[result[1]] else result[1]}
        if coin == self.ccy and "usdt" in maybe.values():
            return
        position.loc[num, "side"] = "long" if self.now_position.loc[coin, maybe["master"]] > 0 else "short"
        if maybe["master"].split("-")[0] != "usd":
            position.loc[num, "position"] = abs(self.now_position.loc[coin, maybe["master"]])
        elif maybe["master"] in self.usd_position.columns:
            position.loc[num, "position"] = abs(self.usd_position.loc[coin, maybe["master"]])
        else:
            position.loc[num, "position"] = np.nan
        if position.loc[num ,"position"] == 0:
            position.loc[num, "MV"] = 0
        else:
            position.loc[num, "MV"] = position.loc[num, "position"] * self.get_coin_price(coin = coin.lower()) if maybe["master"].split("-")[0] != "usd" else position.loc[num, "position"] * self.contractsize_cswap[coin]
        position.loc[num, "MV%"] = round(position.loc[num, "MV"] / self.adjEq * 100, 4)
        position.loc[num, ["master_pair", "slave_pair"]] = [f'{position.loc[num, "coin"]}-{self.coin_master["master"]}', f'{position.loc[num, "coin"]}-{self.coin_master["slave"]}']
        position.loc[num, ["master_secret", "slave_secret"]] = [f'{self.parameter_name}{self.secret_id[self.coin_master["master"]]}', f'{self.parameter_name}{self.secret_id[self.coin_master["slave"]]}']
        position.loc[num, "combo"] = self.get_coin_combo(coin, position.loc[num, "master_pair"], position.loc[num, "slave_pair"])
    
    def get_known_coin_position(self) -> None:
        position, num, coin = self.position, len(self.position), self.execute_coin
        position.loc[num, "coin"] = coin.lower()
        position.loc[num, "side"] = "long" if self.now_position.loc[self.execute_coin, self.coin_master["master"]] > 0 else "short"
        position.loc[num, "position"] = abs(self.now_position.loc[self.execute_coin, self.coin_master["master"]]) if self.coin_master["master"].split("-")[0] != "usd" else abs(self.usd_position.loc[self.execute_coin, self.coin_master["master"]])
        if position.loc[num ,"position"] == 0:
            position.loc[num, "MV"] = 0
        else:
            position.loc[num, "MV"] = position.loc[num, "position"] * self.get_coin_price(coin = coin.lower()) if self.coin_master["master"].split("-")[0] != "usd" else position.loc[num, "position"] * self.contractsize_cswap[coin]
        position.loc[num, "MV%"] = round(position.loc[num, "MV"] / self.adjEq * 100, 4)
        position.loc[num, ["master_pair", "slave_pair"]] = [f'{position.loc[num, "coin"]}-{self.coin_master["master"]}', f'{position.loc[num, "coin"]}-{self.coin_master["slave"]}']
        position.loc[num, ["master_secret", "slave_secret"]] = [f'{self.parameter_name}{self.secret_id[self.coin_master["master"]]}', f'{self.parameter_name}{self.secret_id[self.coin_master["slave"]]}']
        position.loc[num, "combo"] = self.get_coin_combo(coin, position.loc[num, "master_pair"], position.loc[num, "slave_pair"])
    
    def get_eth_coin_position(self) -> None:
        n = self.position.index[-1]
        if self.position.loc[n, "combo"] == "okex_spot-okex_spot":
            self.position.loc[n, ["master_pair", "slave_pair"]] = ["beth-usdt", "eth-usdt"]
        else:
            for col in ["master_pair", "slave_pair"]:
                self.position.loc[n, col] = self.position.loc[n, col] if self.position.loc[n, col] == "beth-usdt" else self.position.loc[n, col].replace("beth", "eth")
    
    def get_account_position(self, timestamp = "10m", the_time = "now()") -> pd.DataFrame:
        self.get_equity()
        data = self.get_now_position(timestamp=timestamp, the_time=the_time).drop(["diff", "diff_U", "is_exposure"], axis=1)
        self.position = pd.DataFrame(columns = ["coin", "side", "position", "MV", "MV%", "master_pair", "slave_pair", "master_secret", "slave_secret", "combo"])
        coins = list(data.index.drop("BETH") if "BETH" in data.index else data.index)
        for coin in coins:
            self.execute_coin = coin.upper()
            self.coin_master = self.tell_coin_master()
            if "" in self.coin_master.values():
                self.get_unknown_coin_position()
            else:
                self.get_known_coin_position()
            self.get_eth_coin_position() if self.execute_coin == "BETH" else None
        self.position = self.position.drop(self.position[((self.position["master_secret"] == self.parameter_name) | (self.position["slave_secret"] == self.parameter_name)) & (self.position["MV%"] < self.ignore_mv)].index)
        self.position = self.position.drop(self.position[(self.position["coin"] == self.ccy.lower()) & (self.position["combo"].isnull())].index).sort_values(by = "MV%", ascending= False)
        self.position.index = range(len(self.position))
        return self.position.copy()
    
    def select_orders(self) -> None:
        ret = pd.concat(self.orders.values()).drop_duplicates(subset = ["market_oid"]).sort_values(by = "dt").reset_index(drop = True)
        ret["coin"] = ret["pair"].apply(lambda x: x.split("-")[0].upper())
        # coin_pair = {coin: ret[ret["coin"] == coin]["pair"].unique() for coin in ret["coin"].unique()}
        # for coin, pair in coin_pair.items():
        #     if len(pair) % 2 ==1 :
        #         ret = ret[ret["pair"] != f"{coin.lower()}-usdt"].copy()
        return ret
    
    def get_usd_number(self, side: str, number: float, avg_price: float, pnl: float) -> float:
        if side.lower() in ["openlong", "closeshort"]:
            ret = number / avg_price - pnl
        elif side.lower() in ["openshort", "closelong"]:
            ret =  number / avg_price + pnl
        else:
            ret = np.nan
        return ret
    
    def get_real_number(self, side: str, number: float) -> float:
        if side.lower() in ["openlong", "closeshort"]:
            ret = - number
        elif side.lower() in ["openshort", "closelong"]:
            ret = number
        else:
            ret = np.nan
        return ret
    
    def handle_orders_data(self, play=False):
        data = pd.DataFrame(columns = ["UTC", "dt", "pair", "coin", "avg_price", "cum_deal_base", "side","turnover","status","exchange", "field", "market_oid", "settlement", "raw", "contractsize", "pnl", "is_usd", "number", "real_number", "fee", "feeCcy","fee_U"])
        raw = self.select_orders()
        if not hasattr(self, "orders") or len(raw) == 0:
            print(f"{self.parameter_name} doesn't have orders data between {self.start} and {self.end}")
            self.trade_data = data.copy()
            return data
        names = ["dt", "pair", "avg_price", "cum_deal_base","side", "exchange", "field",  "status", "settlement", "market_oid", "coin", "raw"]
        data[names], data["UTC"] = raw[names], raw["update_iso"]
        data["contractsize"] = data["pair"].apply(lambda x: self.dataokex.get_contractsize(x))
        data["pnl"] = data["raw"].apply(lambda x: eval(eval(x)["pnl"]))
        data["is_usd"] = data["pair"].apply(lambda x: True if x.lower().split("-")[1] == "usd" else False)
        data["number"] = data["cum_deal_base"].abs() * data["contractsize"]
        data["number"] = data.apply(lambda x: self.get_usd_number(x["side"], x["number"], x["avg_price"], x["pnl"]) if x["is_usd"] else x["number"], axis = 1)
        data["real_number"] = data.apply(lambda x: self.get_real_number(x["side"], x["number"]), axis = 1)
        data["turnover"] = data["real_number"] * data["avg_price"]
        data["fee"], data["feeCcy"] = data["raw"].apply(lambda x: eval(eval(x)["fee"])), data["raw"].apply(lambda x: eval(x)["feeCcy"])
        data["fee_U"] = data.apply(lambda x: x["fee"] if x["feeCcy"].upper() in ["USDT", "USD", "USDC", "BUSD", "USDK"] else x["fee"] * x["avg_price"], axis = 1)
        data = data.sort_values(by = "dt").reset_index(drop=True)
        if play:
            result = pd.DataFrame(columns = ["turnover"], data = data["turnover"]).cumsum()
            p0 = draw_ssh.line(result, x_axis_type = "linear", play = False, title = f"{self.parameter_name}: {self.start} to {self.end}", tips=[('x', '$x{0}'), ('value','$y{0.0000}'),('name','$name'), ('time', '@dt{%Y-%m-%d %H:%M:%S}')], formatters={"@x": "printf", "@dt": "datetime"}, tags = ["dt"])
            tab0 = Panel(child=p0, title="Overview")
            coins = list(data.coin.unique())
            ps, ts = {}, {}
            for i in range(len(coins)):
                a = data[data["coin"] == coins[i]].copy()
                result = pd.DataFrame(columns = ["turnover", "number"])
                for j in a.index:
                    if a.loc[j, "side"] in ["openlong", "closeshort"]:
                        a.loc[j, "number"] = -a.loc[j, "number"]
                result["turnover"] = a["turnover"]
                result["number"] = a["number"]
                result.index = range(len(result))
                result = result.cumsum()
                result = result.fillna(0)
                result["dt"] = data["dt"]
                ps[i] = draw_ssh.line_doubleY(result, right_columns = ["number"],x_axis_type = "linear", play = False, title =f"{self.parameter_name}: {self.start} to {self.end}", tips=[('x', '$x{0}'), ('value','$y{0.0000}'),('name','$name'), ('time', '@dt{%Y-%m-%d %H:%M:%S}')], formatters={"@x": "printf", "@dt": "datetime"}, tags = ["dt"])
                ts[i] = Panel(child = ps[i], title=coins[i])
            tabs = []
            tabs.append(tab0)
            for i in ts.keys():
                tabs.append(ts[i])
            t = Tabs(tabs= tabs)
            show(t)
        self.trade_data = data.copy()
        return data
    
    def get_funding(self, combo: str, start: datetime.date, end: datetime.date, input_coins = []) -> pd.DataFrame:
        master, slave = combo.split("-")
        exchange1, kind1 = master.split("_")[:2]
        exchange2, kind2 = slave.split("_")[:2]
        funding_summary, funding, _ = eva.run_funding(exchange1, kind1, exchange2, kind2, start, end, input_coins = input_coins)
        return funding_summary, funding