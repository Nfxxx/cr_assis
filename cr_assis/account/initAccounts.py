from cr_assis.load import *
import pandas as pd
from cr_assis.account.accountBase import AccountBase
from cr_assis.account.accountOkex import AccountOkex
from cr_assis.account.accountBinance import AccountBinance
from pymongo import MongoClient
from pathlib import Path

class InitAccounts(object):
    
    def __init__(self, combo = "", ignore_test = True) -> None:
        """
        initilize account about some combo
        Args:
            combo (str, optional): strategy name, "" means initilize all strategies. Defaults to "".
        """
        self.ignore_test = ignore_test
        self.combo = combo
        self.script_path = str(Path( __file__ ).parent.parent.absolute())
    
    def get_all_deploys(self) -> list[str]:
        """获得所有启动的账户deploy_id"""
        mongo_clt = MongoClient(os.environ["MONGO_URI"])
        collections = mongo_clt["Strategy_orch"].list_collection_names()
        deploy_ids = []
        for key in collections:
            a = mongo_clt["Strategy_orch"][key].find()
            data = pd.DataFrame(a)
            data = data[(data["orch"]) & (data["version"] != "0") & (data["version"] != None)].copy()
            deploy_ids += list(data["_id"].values)
        deploy_ids.sort()
        self.deploy_ids = deploy_ids
        return deploy_ids

    def init_accounts(self, is_usdc = False) -> dict[str, AccountBase]:
        self.load_combo_deployId() if (not hasattr(self, "combo_deployId") and self.combo != "") else None
        strategy = self.combo_deployId[self.combo] if self.combo != "" else ""
        deploy_ids = self.get_all_deploys()
        accounts = {}
        for deploy_id in deploy_ids:
            parameter_name, _ = deploy_id.split("@")
            client, _ = parameter_name.split("_")
            tell1 = not (self.ignore_test and client in ["test", "lxy"])
            tell2 = False
            for i in strategy:
                if i in deploy_id:
                    tell2 = True
                    break
            if tell1 and tell2:
                accounts[parameter_name] = AccountBase(deploy_id = deploy_id, is_usdc= is_usdc)
        self.accounts = accounts.copy()
        return accounts
    
    def init_accounts_binance(self):
        deploy_ids = self.get_all_deploys()
        accounts:dict[str, AccountBinance] = {}
        for deploy_id in deploy_ids:
            parameter_name, strategy = deploy_id.split("@")
            client, _ = parameter_name.split("_")
            if not (self.ignore_test and client in ["test", "lxy"]) and ("pt" == strategy.split("_")[0] or "dt" == strategy.split("_")[0]) and "binance" == strategy.split("_")[1]:
                accounts[parameter_name] = AccountBinance(deploy_id = deploy_id)
        self.accounts = accounts.copy()
        return accounts
    
    def init_accounts_okex(self) -> dict[str, AccountOkex]:
        deploy_ids = self.get_all_deploys()
        accounts:dict[str, AccountOkex] = {}
        for deploy_id in deploy_ids:
            parameter_name, strategy = deploy_id.split("@")
            client, _ = parameter_name.split("_")
            if not (self.ignore_test and client in ["test", "lxy"]) and "okex" in strategy:
                accounts[parameter_name] = AccountOkex(deploy_id = deploy_id)
        self.accounts = accounts.copy()
        return accounts
    
    def load_combo_deployId(self):
        path = self.script_path + "/config"
        with open(f"{path}/combo_deployId.json", "r") as f:
            self.combo_deployId = json.load(f)