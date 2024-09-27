
from cr_assis.pnl.okexPnl import OkexPnl
from binance.um_futures import UMFutures
from cr_assis.load import *
from cr_assis.connect.connectData import ConnectData
from binance.cm_futures import CMFutures
from cr_assis.draw import draw_ssh
from cr_assis.account.accountBinance import AccountBinance
from urllib.parse import urljoin, urlencode
import requests, time, hmac, hashlib

class BinancePnl(OkexPnl):
    
    def __init__(self):
        self.database = ConnectData()
        self.ccy = "USDT"
        self.interval = "1H"
        self.exchange = "binance"
        self.combo = "binance_usd_swap-binance_usdt_swap"
        self.slip_unit = 10000
        self.real_uswap = 0.0004
        self.real_cswap = 0.0001
        self.fake_uswap = 0.00013
        self.fake_cswap = -0.00009
        self.api_info = {}
        self.mv = {}
        self.get_accounts()
    
    def get_accounts(self) -> None:
        with open(f"{os.environ['HOME']}/.cr_assis/account_binance_api.yml", "rb") as f:
            data: list[dict] = yaml.load(f, Loader= yaml.SafeLoader)
        self.accounts = [i['name'] for i in data if "hf" == i['name'].split("_")[0]]
        for i in data:
            self.api_info[i["name"]] = {"api_key": i["api_key"], "secret_key": i["secret_key"]}
    
    def load_api_uswap(self, name: str) -> None:
        self.api_uswap = UMFutures(self.api_info[name]["api_key"], self.api_info[name]["secret_key"])
    
    def load_api_cswap(self, name: str) -> None:
        self.api_cswap = CMFutures(self.api_info[name]["api_key"], self.api_info[name]["secret_key"], base_url="https://dapi.binance.com")
    
    def get_long_bills(self, name: str, start: datetime.datetime, end: datetime.datetime) -> pd.DataFrame:
        start = self.dt_to_ts(start)
        ts = self.dt_to_ts(end)
        self.load_api_uswap(name)
        data = []
        while ts >= start:
            print(ts)
            response = self.api_uswap.get_account_trades(symbol = "BTCUSDT", recvWindow=6000, endTime = ts)
            data += response
            if len(response) > 0:
                ts = response[0]["time"]
            else:
                break
        ts = self.dt_to_ts(end)
        self.load_api_cswap(name)
        while ts >= start:
            response = self.api_cswap.get_account_trades(pair = "BTCUSD", recvWindow=6000, endTime = ts)
            data += response
            if len(response) > 0:
                ts = response[0]["time"]
            else:
                break
        df = pd.DataFrame(data).drop_duplicates().sort_values(by = "time")
        df["dt"] = df["time"].apply(lambda x: self.ts_to_dt(x))
        cols = ["price", "qty", "realizedPnl", "quoteQty", "commission", "baseQty"]
        df[cols] = df[cols].astype(float)
        df.index = range(len(df))
        return df
    
    def handle_bills(self, data: pd.DataFrame, is_play = True) -> pd.DataFrame:
        data["coin"] = data["symbol"].apply(lambda x: x.split("USD")[0].upper())
        for i in data.index:
            data.loc[i, "marginPrice"] = 1 if data.loc[i, "marginAsset"] in ["USDT", "USD", "USDK", "BUSD", "USDC"] else data.loc[i, "price"]
            data.loc[i, "commissionPrice"] = 1 if data.loc[i, "commissionAsset"] in ["USDT", "USD", "USDK", "BUSD", "USDC"] else data.loc[i, "price"]
            data.loc[i, "fake_commission"] = self.fake_uswap * data.loc[i, "quoteQty"] if np.isnan(data.loc[i, "baseQty"]) else self.fake_cswap * data.loc[i, "baseQty"]
        data["pnl"] = data["realizedPnl"] * data["marginPrice"]
        data["fee"] = - data["commission"] * data["commissionPrice"]
        data["fake_fee"] = - data["fake_commission"] * data["commissionPrice"]
        data["balChg_U"] = data["pnl"] + data["fee"]
        data["fake_balChg_U"] = data["pnl"] + data["fake_fee"]
        data["cum_pnl"] = data["balChg_U"].cumsum()
        data["fake_cum_pnl"] = data["fake_balChg_U"].cumsum()
        if is_play:
            result = data[["dt", "cum_pnl", "fake_cum_pnl"]].copy()
            result.set_index("dt",inplace=True)
            p = draw_ssh.line(result)
        self.bills_data = data
        return data
    
    def get_deal(self, deploy_id: str, start: datetime.datetime, end: datetime.datetime) -> dict:
        account = AccountBinance(deploy_id)
        data = account.get_history_parameter()
        data = data[(data["dt"] >= start)&(data["dt"] <= end)].copy()
        parameter: dict[str, pd.DataFrame] = {}
        spreads: dict[str, pd.DataFrame] = {}
        result: dict[str, pd.DataFrame] = {}
        self.reach: dict[str, pd.DataFrame] = {}
        for i in data.index:
            dt = data.loc[i, "dt"]
            paras = data.loc[i, "spreads"]
            for pair, v in paras.items():
                coin = pair.split("-")[0].upper()
                df = pd.DataFrame(columns = ["bid", "ask"]) if coin not in parameter.keys() else parameter[coin]
                if "long" in v.keys():
                    df.loc[dt] = [v["long"][0]["open"], v["long"][0]["close_maker"]]
                else:
                    df.loc[dt] = [v["short"][0]["close_maker"], v["long"][0]["open"]]
                parameter[coin] = df
        self.is_reach = {}
        for coin in parameter.keys():
            spreads[coin] = account.get_spreads(coin, combo = self.combo, start = f"'{start+datetime.timedelta(hours = -8)}'", end = f"'{end+datetime.timedelta(hours = -8)}'")
            spreads[coin].index = spreads[coin]["time"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ") + datetime.timedelta(hours = 8))
            result[coin] = pd.merge(spreads[coin], parameter[coin], left_index=True, right_index=True, how="outer").fillna(method="ffill").dropna()
            result[coin]["is_reach"] = (result[coin]["bid0_spread"] >= result[coin]["bid"]) | (result[coin]["ask0_spread"] >= result[coin]["ask"])
            self.reach[coin] = result[coin]["is_reach"].resample(self.interval).mean()
            self.is_reach[coin] = result[coin][["is_reach"]]
        return result
    
    def get_orders(self, name: str, start: datetime.datetime, end: datetime.datetime) -> dict:
        self.load_api_cswap(name)
        start = self.dt_to_ts(start)
        ts = self.dt_to_ts(end)
        data = []
        while ts >= start:
            try:
                response = self.api_cswap.get_all_orders(symbol="BTCUSD_PERP", recvWindow=2000, limit = 100, endTime = ts)
            except:
                time.sleep(1)
            data += response
            ts = response[0]["updateTime"]
            if len(response) > 0:
                ts = response[0]["updateTime"]
            else:
                break
        df = pd.DataFrame(data).drop_duplicates()
        df["dt"] = df["time"].apply(lambda x: datetime.datetime.fromtimestamp(float(x)/1000))
        df = df[(df["dt"] >= self.ts_to_dt(start)) & (df["dt"] <= end)].copy()
        df["coin"] = df["symbol"].apply(lambda x: x.split("USD")[0].upper())
        df["is_maker"] = True
        df["is_trade"] = (df["status"] != "CANCELED") & (df["status"] != "EXPIRED")
        self.is_maker, self.is_trade = {}, {}
        self.order = {}
        for coin in df["coin"].unique():
            self.order[coin] = pd.DataFrame(columns = ["is_maker", "is_trade"])
            self.is_maker[coin] = df[df["coin"] == coin].set_index("dt")[["is_maker"]].resample("1Min").sum()
            self.is_trade[coin] = df[df["coin"] == coin].set_index("dt")[["is_trade"]].resample("1Min").sum()
            self.order[coin]["is_maker"] = self.is_maker[coin]["is_maker"].apply(lambda x: True if x > 0 else False)
            self.order[coin]["is_trade"] = self.is_trade[coin]["is_trade"].apply(lambda x: True if x > 0 else False)
        return df
    
    def get_uswap_funding(self, name: str, start_ts: str, end_ts: str) -> pd.DataFrame:
        self.load_api_uswap(name)
        ts = end_ts
        data = []
        while ts >= start_ts:
            try:
                response = self.api_uswap.get_income_history(incomeType = "FUNDING_FEE", recvWindow=2000, limit = 100, endTime = ts)
            except:
                time.sleep(1)
            data += response
            ts = response[0]["updateTime"]
            if len(response) > 0:
                ts = response[0]["updateTime"]
            else:
                break
    
    def get_funding_income(self, name: str, start: datetime.datetime, end: datetime.datetime) -> pd.DataFrame:
        start_ts, end_ts = self.dt_to_ts(start_ts), self.dt_to_ts(end_ts)
        self.get_uswap_funding(name, start_ts, end_ts)
    