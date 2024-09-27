from binance.um_futures import UMFutures
import os, yaml, datetime
import pandas as pd

class BinanceWallet(object):
    
    def __init__(self) -> None:
        self.file_path = "/mnt/efs/fs1/data_ssh/mm/binance/total" if os.path.exists("/mnt/efs/fs1/data_ssh/mm/binance/total") else os.environ["HOME"] + "/data/mm/binance/total"
        self.name = ""
        self.equity_usdt = {}
        self.position = {}
        self.api_info = {}
    
    def get_accounts(self) -> None:
        with open(f"{os.environ['HOME']}/.cr_assis/account_binance_api.yml", "rb") as f:
            data: list[dict] = yaml.load(f, Loader= yaml.SafeLoader)
        self.accounts = [i['name'] for i in data if "hf" == i['name'].split("_")[0]]
        for i in data:
            self.api_info[i["name"]] = {"api_key": i["api_key"], "secret_key": i["secret_key"]}
    
    def get_wallet_equity(self) -> float:
        self.equity_usdt[self.name] = self.account_api.account(recvWindow=6000)
        for i in self.equity_usdt[self.name]["assets"]:
            if i["asset"] == "USDT":
                return float(i["marginBalance"])
    
    def get_wallet_position(self) -> float:
        self.position[self.name] = self.account_api.get_position_risk(recvWindow=6000)
        position = 0
        for i in self.position[self.name]:
            position += float(i["notional"])
        return position
    
    def update_wallet(self) -> None:
        self.get_accounts()
        self.now = datetime.datetime.utcnow().replace(microsecond=0)
        self.result = {}
        for name in self.accounts:
            self.name = name
            self.account_api = UMFutures(key = self.api_info[name]["api_key"], secret= self.api_info[name]["secret_key"])
            save_path = f"{self.file_path}/{name}"
            os.makedirs(save_path) if not os.path.exists(save_path) else None
            self.result[name] = {"equity" : self.get_wallet_equity(), "position_value": self.get_wallet_position()}
            self.result[name]["mv%"] = self.result[name]["position_value"] / self.result[name]["equity"]
            data = pd.DataFrame.from_dict({self.now:self.result[name]}, orient="index")
            data.to_csv(f"{save_path}/{self.now.date()}.csv", mode = "a", header= (not os.path.isfile(f"{save_path}/{self.now.date()}.csv")))