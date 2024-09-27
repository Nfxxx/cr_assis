from cr_assis.load import *
import requests

class EvaDT(object):
    """DT Funding Evaluation
    """
    
    def __init__(self, csv_path: str, expect_profit = 0.0024, interval = 30) -> None:
        """
        Args:
            csv_path (str): file path of funding data csv
            expect_profit (float, optional): the funding profit where user wants to have. Defaults to 0.0024.
            interval (int, optional): the amount of days. Defaults to 30.
        """
        self.master = "USDT-SWAP"
        self.slave = "USD-SWAP"
        self.url = "https://www.okx.com/api/v5/market/history-candles?"
        self.csv_path = csv_path
        self.expect_profit = expect_profit
        self.interval = interval
        self.number = self.interval * 3
        self.load_funding_data()
        
    def load_funding_data(self):
        data = pd.read_csv(self.csv_path, index_col=0)
        data.index = pd.to_datetime(data.index)
        data = data.resample("1D").sum()
        self.funding = data
    
    def get_funding_profit(self):
        self.load_funding_data() if not hasattr(self, "funding") else None
        self.funding_profit = self.funding[["BTC", "ETH"]].rolling(self.interval).sum().shift(- self.interval)
        self.funding_profit["funding"] = abs(self.funding_profit).max(axis = 1)
        for i in self.funding_profit.index:
            if abs(self.funding_profit.loc[i, "BTC"]) == self.funding_profit.loc[i, "funding"]:
                coin = "BTC"
            else:
                coin = "ETH"
            self.funding_profit.loc[i, "coin"] = coin
            if self.funding_profit.loc[i, coin] >= 0:
                self.funding_profit.loc[i, "is_long"] = False
            else:
                self.funding_profit.loc[i, "is_long"] = True
        self.profit = pd.DataFrame(columns = ["funding", "coin", "is_long"]) if not hasattr(self, "profit") else self.profit
        cols = list(self.profit.columns)
        self.profit[cols] = self.funding_profit[cols]
    
    def get_spread_cost(self):
        for timestamp in self.profit.index:
            if self.profit.loc[timestamp, "funding"] >= self.expect_profit:
                spread = self.get_spread(coin = self.profit.loc[timestamp, "coin"], timestamp=timestamp)
                self.profit.loc[timestamp, "spread"] = spread
                if not self.profit.loc[timestamp, "is_long"]:
                    spread_cost = self.profit.loc[timestamp, "spread"] - 1
                else:
                    spread_cost = 1 - self.profit.loc[timestamp, "spread"]
                self.profit.loc[timestamp, "cost"] = spread_cost
    
    def _send_requests(self, url: str) -> dict:
        headers = {
            "accept": "application/json",
            "content-type": "application/json"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            ret = response.json()
        else:
            ret = {"data": []}
        return ret
    
    def get_kline(self, coin: str, timestamp: datetime.date) -> dict:
        ts = int(datetime.datetime.timestamp(datetime.datetime.combine(timestamp, datetime.datetime.min.time())) * 1000)
        kline = {}
        for contract in [self.master, self.slave]:
            url = f"{self.url}instId={coin.upper()}-{contract}&bar=15m&after={ts}"
            ret = self._send_requests(url)
            kline[contract] = pd.DataFrame(ret["data"])
            kline[contract].columns = ["ts", f"open_{contract}", f"high_{contract}", f"lower_{contract}", f"close_{contract}", f"vol_{contract}", f"volCcy_{contract}", f"volCcyQuote_{contract}", f"confirm_{contract}"]
        return kline
    
    def get_spread(self, coin: str, timestamp: datetime.date) -> float:
        kline = self.get_kline(coin, timestamp)
        result = pd.merge(kline[self.master], kline[self.slave], on="ts")
        result["spread"] = result[f"close_{self.master}"].astype(float) / result[f"close_{self.slave}"].astype(float)
        spread = np.mean(result["spread"])
        return spread
        