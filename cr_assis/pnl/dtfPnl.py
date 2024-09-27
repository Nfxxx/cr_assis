from cr_assis.pnl.fsoPnl import FsoPnl
from cr_assis.account.accountBase import AccountBase
from cr_assis.connect.connectData import ConnectData
import pandas as pd
import numpy as np
import datetime

class DtfPnl(FsoPnl):
    
    def __init__(self, accounts: list) -> None:
        super().__init__(accounts)
    
    def get_locked_tpnl(self, account: AccountBase) -> dict:
        account.get_now_position(timestamp = "5m") if not hasattr(account, "now_position") else None
        coin = account.principal_currency.lower()
        locked_tpnl = {}
        sql = f"""
        SELECT (last(long) - last(short)) * 100 / (last(long_open_price) + last(short_open_price)) as tpnl FROM "position" 
        WHERE time > now() - 5m and username = '{account.username}'and client = '{account.client}' and pair = '{coin}-usd-swap'
        """
        data = self.database._send_influx_query(sql, database = "account_data")
        if len(data) > 0:
            tpnl = data["tpnl"].values[-1]
        else:
            tpnl = np.nan
        locked_tpnl[coin] = tpnl
        account.locked_tpnl = locked_tpnl
        return locked_tpnl