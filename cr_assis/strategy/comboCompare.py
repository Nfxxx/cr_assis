from research.eva import eva
import pandas as pd
import numpy as np
import datetime, sys, os
from cr_assis.connect.connectData import ConnectData

class ComboCompare(object):
    """compare different comboes' chance in okex"""
    
    def __init__(self, trade_fee = 0.0004, interval = "6h") -> None:
        self.coins = ["BTC", "ETH"]
        self.mul = {"ssf": 1.2, "dt": 2.5, "fs": 2.5}
        self.interest = 0.03
        self.trade_fee = trade_fee
        self.interval = interval
        self.exchange = "okex"
        self.database = ConnectData()
        self.eva = eva
        self.funding = {}
        self.today = datetime.date.today()
        self.end_date = self.today
        self.start_date = self.end_date + datetime.timedelta(days = -30)
    
    def get_quarter(self) -> str:
        today = self.end_date
        quarter = str(today.year)[-2:]
        last_day = ""
        month = today.month
        if month <= 3:
            last_day = "0331"
            last_date = datetime.date(today.year, 3, 15)
        elif month <= 6:
            last_day = "0630"
            last_date = datetime.date(today.year, 6, 15)
        elif month <= 9:
            last_day = "0930"
            last_date = datetime.date(today.year, 9, 15)
        else:
            last_day = "1231"
            last_date = datetime.date(today.year, 12, 15)
        quarter = quarter + last_day
        self.quarter = quarter
        self.last_date = last_date
        return quarter
    
    def get_ssf_chance(self):
        funding_sum, funding_diff, _ = self.eva.run_funding(self.exchange, "spot", self.exchange, "usdt", self.start_date, self.end_date, play = False, input_coins=self.coins)
        self.ssf_chance = funding_sum
        self.funding["ssf"] = funding_diff
    
    def get_dt_chance(self):
        funding_sum, funding_diff, _ = self.eva.run_funding(self.exchange, "usdt", self.exchange, "usd", self.start_date, self.end_date, play = False, input_coins=self.coins)
        self.dt_chance = funding_sum
        self.funding["dt"] = funding_diff
    
    def get_thresh(self, spreads: pd.DataFrame, threshold = 50) -> float:
        spreads_avg = np.mean(spreads)
        spreads_minus_mean = spreads - spreads_avg
        up_amp = spreads_minus_mean.iloc[np.where(spreads_minus_mean>0)]
        up_thresh = float(np.percentile(up_amp,[threshold]) + spreads_avg)
        return up_thresh
    
    def get_spread_profit(self):
        this_quarter = self.get_quarter()
        self.spread = {}
        self.spread_profit = {}
        for coin in self.coins:
            spread = {}
            spread_profit = {}
            for contract in ["usdt", "usd"]:
                sql = f"""
                SELECT ask0_spread as spread FROM "spread_orderbook_okex_futures_{coin.lower()}_{contract}_{this_quarter}__orderbook_okex_spot_{coin.lower()}_usdt" WHERE time > now() - {self.interval}
                """
                ret = self.database._send_influx_query(sql, database = "spreads", is_dataFrame = True)
                if len(ret) > 0:
                    spread[contract] = self.get_thresh(ret["spread"])
                else:
                    spread[contract] = 1
                spread_profit[contract] = abs(spread[contract] - 1)
            max_contract = "usdt" if spread_profit["usdt"] >= spread_profit["usd"] else "usd"
            self.spread[coin]= spread[max_contract]
            self.spread_profit[coin] = spread_profit[max_contract]
        
    def get_month_spread(self):
        self.get_spread_profit() if not hasattr(self, "spread_profit") else None
        days = (self.last_date - self.today).days
        self.month_spread = {}
        for coin in self.spread_profit.keys():
            self.month_spread[coin] = self.spread_profit[coin] / days * 30
    
    def originze_result(self):
        result = pd.DataFrame(columns = ["ssf", "dt", "spread", "fs", "fee_ssf", "fee_fs"])
        self.get_dt_chance() if not hasattr(self, "dt_chance") else None
        self.get_ssf_chance() if not hasattr(self, "ssf_chance") else None
        self.get_month_spread() if not hasattr(self, "month_spread") else None
        for coin in self.coins:
            result.loc[coin, "ssf"] = (self.ssf_chance.loc[coin, "30d"] - self.interest / 12) * self.mul["ssf"]
            result.loc[coin, "dt"] = (self.dt_chance.loc[coin, "30d"]) * self.mul["dt"]
            result.loc[coin, "spread"] = self.month_spread[coin]
            result.loc[coin, "fs"] = result.loc[coin, "spread"] - self.ssf_chance.loc[coin, "30d"] if self.spread[coin] > 1 else result.loc[coin, "spread"] + self.ssf_chance.loc[coin, "30d"]
        result["fs"] = result["fs"] * self.mul["fs"]
        result["fee_ssf"] = self.trade_fee * (self.mul["dt"] + self.mul["ssf"])
        result["fee_fs"] = self.trade_fee * (self.mul["dt"] + self.mul["fs"])
        self.result = result.copy()
        format_dict = {}
        for col in result.columns:
            format_dict[col] = '{0:.4%}'
        result = result.style.format(format_dict)
        return result