from cr_assis.api.okex.marketApi import MarketAPI
from cr_assis.api.okex.publicApi import PublicAPI
import os, json, copy, glob, datetime, time, pytz
import pandas as pd

class UpdateOkexMarket(object):
    
    def __init__(self) -> None:
        self.save_path = "/mnt/efs/fs1/data_ssh"
        self.tickers_path = "/tickers/okex"
        self.interest_path = "/interest/okex"
        self.fake_df = pd.DataFrame.from_dict({0:{"dt": "2021-01-01 00:00:00", "ts": "1609459200000"}}, orient="index")
        self.market_api = MarketAPI()
        self.public_api = PublicAPI()
    
    def get_margin_coins(self) -> set[str]:
        response = self.public_api.get_instruments(instType="MARGIN")
        ret = response.json() if response.status_code == 200 else {"data": []}
        self.margin_coins = set([i["baseCcy"] for i in ret["data"] if i["state"] == "live"]) | set(["USDT", "USDC"])
        return copy.deepcopy(self.margin_coins)
    
    def get_last_ts(self, path: str) -> str:
        """
        Args:
            path (str): file path, including many csv files, named by date
        Returns:
            str: last datetime.datetime, UTC+8, %Y-%m-%d %H:%M:%S
        """
        files = glob.glob(f"{path}/*.csv") if os.path.exists(path) else []
        files.sort()
        df = pd.read_csv(files[-1], index_col=0) if len(files) > 0 else self.fake_df
        df = df.sort_values(by = "ts") if "ts" in df.columns else self.fake_df
        return df["ts"].values[-1]
    
    def transfer_ts_utc(self, ts: str) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(float(ts) / 1000).astimezone(datetime.timezone.utc)
    
    def transfer_ts_utc8(self, ts: str) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(float(ts) / 1000).astimezone(pytz.timezone("Asia/ShangHai"))
    
    def handle_origin_data(self, data: list) -> pd.DataFrame:
        df = pd.DataFrame(data)
        if "ts" in df.columns:
            df.sort_values(by = "ts", inplace= True)
            df["timestamp"] = df["ts"].apply(lambda x: self.transfer_ts_utc(x))
            df["dt"] = df["ts"].apply(lambda x: self.transfer_ts_utc8(x))
        return df
    
    def save_as_month(self, df: pd.DataFrame, save_path: str):
        df["month"] = df["dt"].dt.to_period("M")
        for month, data in df.groupby(["month"]):
            save_file = f'{save_path}/{month[0]}.csv'
            data.to_csv(save_file, mode = "a", header= (not os.path.isfile(save_file)))
        
    def update_tickers(self):
        save_path = self.save_path + self.tickers_path if os.path.exists(self.save_path) else os.environ["HOME"] + self.tickers_path
        os.makedirs(save_path) if not os.path.exists(save_path) else None
        for instType in ["SPOT", "SWAP", "FUTURES", "OPTION"]:
            response = self.market_api.get_tickers(instType)
            ret = response.json() if response.status_code == 200 else {"data": []}
            data = {i["instId"]: i for i in ret["data"]}
            if data != {}:
                with open(f"{save_path}/{instType}.json", "w") as f:
                    json.dump(data, f)
    
    def update_coin_interest(self, coin: str):
        coin = coin.upper()
        save_path = self.save_path + self.interest_path + f'/origin/{coin}' if os.path.exists(self.save_path) else f"""{os.environ["HOME"]}/data/{self.interest_path}/origin/{coin}"""
        os.makedirs(save_path) if not os.path.exists(save_path) else None
        start = int(self.get_last_ts(path = f"{save_path}"))
        ts = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        data = []
        ret = {"data": [1]}
        while len(ret["data"]) > 0 and int(ts) > start:
            response = self.market_api.get_lending_history(ccy = coin, after=ts)
            if response.status_code == 429:
                time.sleep(0.1)
                continue
            ret = response.json() if response.status_code == 200 else {"data": []}
            data = data + ret['data']
            ts = ret["data"][-1]["ts"] if len(ret["data"]) > 0 else start
        df = self.handle_origin_data(data)
        self.save_as_month(df, f"{save_path}")
        return df
    
    def update_all_interest(self):
        coins = self.get_margin_coins()
        for coin in coins:
            self.update_coin_interest(coin)
