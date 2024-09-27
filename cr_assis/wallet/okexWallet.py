from cr_assis.api.okex.accountApi import AccountAPI
import os, yaml, datetime, copy
import numpy as np
import pandas as pd

class OkexWallet(object):
    
    def __init__(self) -> None:
        self.account_api = AccountAPI()
        self.file_path = "/mnt/efs/fs1/data_ssh/mm/okex/total" if os.path.exists("/mnt/efs/fs1/data_ssh/mm/okex/total") else os.environ["HOME"] + "/data/mm/okex/total"
    
    def get_accounts(self) -> None:
        with open(f"{os.environ['HOME']}/.cr_assis/account_okex_api.yml", "rb") as f:
            data: list[dict] = yaml.load(f, Loader= yaml.SafeLoader)
        self.accounts = [i['name'] for i in data if "hf" == i['name'].split("_")[0]]
        
    def get_wallet_equity(self) -> float:
        response = self.account_api.get_account_balance()
        ret = response.json() if response.status_code == 200 else {"data": [{"totalEq": "nan"}]}
        return float(ret["data"][0]["totalEq"])
    
    def get_wallet_position(self) -> float:
        response = self.account_api.get_positions()
        ret = response.json() if response.status_code == 200 else {"data": [{'notionalUsd': "nan"}]}
        position = 0
        for i in ret["data"]:
            position += float(i["notionalUsd"]) if i["notionalUsd"] != "" else 0
        return position
    
    def update_wallet(self) -> None:
        self.get_accounts()
        self.now = datetime.datetime.utcnow().replace(microsecond=0)
        self.result = {}
        for name in self.accounts:
            save_path = f"{self.file_path}/{name}"
            os.makedirs(save_path) if not os.path.exists(save_path) else None
            self.account_api.name = name
            self.account_api.load_account_api()
            self.result[name] = {"equity" : self.get_wallet_equity(), "position_value": self.get_wallet_position()}
            self.result[name]["mv%"] = self.result[name]["position_value"] / self.result[name]["equity"]
            data = pd.DataFrame.from_dict({self.now:self.result[name]}, orient="index")
            data.to_csv(f"{save_path}/{self.now.date()}.csv", mode = "a", header= (not os.path.isfile(f"{save_path}/{self.now.date()}.csv")))
