import pandas as pd
import numpy as np
import os, datetime
from cr_assis import draw_ssh
from IPython.display import display

class BackTest(object):
    """BackTest for DT-O
    """
    
    def __init__(self, funding: pd.DataFrame) -> None:
        self.funding = funding
        self.balance = 100.0
        self.trade_fee = 0.0005
        self.mul = 2.5
        self.trade = pd.DataFrame(columns = ["time", "coin", "side"])
        self.position = pd.DataFrame(columns = ["coin", "side", "profit"], index = funding.index)
        self.holding_position = pd.DataFrame(columns = ["coin", "side", "start", "end", "profit", "position_value"])
        self.equity = pd.DataFrame(columns = ["equity"], index = funding.index)
    
        
    def open_position(self, ts):
        num = 45
        least_profit = 0.00003
        funding = self.funding.loc[:ts].copy()
        if len(funding) >= num:
            if np.mean(funding["BTC"].values[-num:]) >= least_profit:
                self.holding_position.loc[0] = ["BTC", "short", ts, None, 0, self.mul * self.balance]
                self.balance -= self.mul * self.trade_fee * self.balance
                self.trade.loc[len(self.trade)] = [ts, "BTC", "open_short"]
            elif np.mean(funding["BTC"].values[-num:]) <=  - least_profit:
                self.holding_position.loc[0] = ["BTC", "long", ts, None, 0, self.mul * self.balance]
                self.balance -= self.mul * self.trade_fee * self.balance
                self.trade.loc[len(self.trade)] = [ts, "BTC", "open_long"]
            elif np.mean(funding["ETH"].values[-num:]) >= least_profit:
                self.holding_position.loc[0] = ["ETH", "short", ts, None, 0, self.mul * self.balance]
                self.balance -= self.mul * self.trade_fee * self.balance
                self.trade.loc[len(self.trade)] = [ts, "ETH", "open_short"]
            elif np.mean(funding["ETH"].values[-num:]) <=  - least_profit:
                self.holding_position.loc[0] = ["ETH", "long", ts, None, 0, self.mul * self.balance]
                self.balance -= self.mul * self.trade_fee * self.balance
                self.trade.loc[len(self.trade)] = [ts, "ETH", "open_long"]
            else:
                pass
        
    def close_position(self, ts):
        num = 30
        least_profit = 0.00003
        funding = self.funding.loc[:ts].copy()
        coin = self.holding_position.loc[0, "coin"]
        if self.holding_position.loc[0, "side"] == "long" and np.mean(funding[coin].values[-num:]) >= least_profit:
            self.balance -= self.trade_fee * self.holding_position.loc[0, "position_value"]
            self.trade.loc[len(self.trade)] = [ts, coin, "close_long"]
            self.holding_position.drop(0, inplace= True)
        elif self.holding_position.loc[0, "side"] == "short" and np.mean(funding[coin].values[-num:]) <=  -least_profit:
            self.balance -= self.trade_fee * self.holding_position.loc[0, "position_value"]
            self.trade.loc[len(self.trade)] = [ts, coin, "close_short"]
            self.holding_position.drop(0, inplace= True)
        else:
            pass
    
    def strategy(self, ts):
        if len(self.holding_position) == 0:
            self.open_position(ts)
        else:
            self.close_position(ts)
    
    def write_position(self, ts):
        if len(self.holding_position) > 0 and self.holding_position.loc[0, "end"] == None:
            coin = self.holding_position.loc[0, "coin"]
            side = self.holding_position.loc[0, "side"]
            if side == "short":
                profit = self.funding.loc[ts, coin] * self.holding_position.loc[0, "position_value"]
            elif side == "long":
                profit = - self.funding.loc[ts, coin]* self.holding_position.loc[0, "position_value"]
            else:
                profit = np.nan
            self.position.loc[ts] = [coin, side, profit]
            self.balance += profit
        else:
            self.position.loc[ts] = ["", "", 0]
    
    def run(self):
        for ts in self.funding.index:
            self.strategy(ts)
            self.write_position(ts)
            self.equity.loc[ts, "equity"] = self.balance

data = pd.read_csv("/Users/ssh/Downloads/funding.csv", index_col=0)
data["timestamp"] = data.index
data["timestamp"] = data["timestamp"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
data.set_index("timestamp", inplace = True)
data.sort_index(inplace = True)
back = BackTest(funding = data)
back.run()
print(back.balance)
display(back.trade)
result = pd.concat([back.equity, back.funding.cumsum() * 2.5])
draw_ssh.line_doubleY(result, right_columns=["equity"], title = "open: 0.00003 close 30: 0.00003")
print(111111)