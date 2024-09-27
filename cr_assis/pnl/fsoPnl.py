from cr_assis.account.accountBase import AccountBase
from cr_assis.connect.connectData import ConnectData
import pandas as pd
import numpy as np
import datetime

class FsoPnl(object):
    """pnl for fs-o"""
    
    def __init__(self, accounts: list) -> None:
        """
        Args:
            accounts (list): list of AccountBase
        """
        self.accounts = accounts
        self.database = ConnectData()
        self.now = datetime.datetime.utcnow()
        self.end_time = self.now + datetime.timedelta(hours = 8)
        
    def get_coin_tpnl(self, account: AccountBase, coin: str) -> float:
        account.get_now_position(timestamp = "5m") if not hasattr(account, "now_position") else None
        now_position = account.now_position
        if coin in now_position.index:
            tpnl = abs((now_position.loc[coin, "master_MV"] / now_position.loc[coin, "master_open_price"]) - (now_position.loc[coin, "slave_MV"] / now_position.loc[coin, "slave_open_price"]))
        else:
            tpnl = 0
        return tpnl
    
    def get_locked_tpnl(self, account: AccountBase) -> dict:
        account.get_now_position(timestamp = "5m") if not hasattr(account, "now_position") else None
        account.get_equity()if not hasattr(account, "adjEq") else None
        now_position = account.now_position
        locked_tpnl = {}
        for coin in now_position.index:
            locked_tpnl[coin] = self.get_coin_tpnl(account, coin) * account.get_coin_price(coin)
        account.locked_tpnl = locked_tpnl
        return locked_tpnl
    
    def get_open_time(self, account: AccountBase) -> datetime.datetime:
        account.get_now_position(timestamp = "5m") if not hasattr(account, "now_position") else None
        position = account.origin_position[account.origin_position["ex_field"] == "futures"].copy()
        if len(position) > 0:
            pair = position["pair"].unique()[0]
            sql = f"""
            select long, short, pair from "position" where pair = '{pair}' and (long >0 or short >0)
            and username = '{account.username}' and client = '{account.client}'
            order by time LIMIT 1
            """
            df = self.database._send_influx_query(sql, database = "account_data", is_dataFrame= True)
            open_time = datetime.datetime.strptime(df["time"].values[0][:19].replace("T", " "), "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours = 8)
        else:
            open_time = self.end_time
        account.start = open_time
        account.end = self.end_time
        return open_time
    
    def get_fpnl(self, account: AccountBase) -> dict:
        self.get_open_time(account) if not hasattr(account, "start") else None
        third_pnl = account.get_third_pnl()
        return third_pnl
    
    def get_pnl(self):
        for account in self.accounts:
            self.get_locked_tpnl(account) if not hasattr(account, "locked_tpnl") else None
            self.get_fpnl(account) if not hasattr(account, "third_pnl") else None