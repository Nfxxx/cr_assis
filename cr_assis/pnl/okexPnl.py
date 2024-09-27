from cr_assis.api.okex.accountApi import AccountAPI
from cr_assis.api.okex.tradeApi import TradeAPI
from cr_assis.load import *
from cr_assis.connect.connectOkex import ConnectOkex
from cr_assis.connect.connectData import ConnectData
from cr_assis.account.accountOkex import AccountOkex
from cr_assis.draw import draw_ssh
from bokeh.models import NumeralTickFormatter
from bokeh.plotting import show
from bokeh.models.widgets import Panel, Tabs
import pytz

class OkexPnl(object):
    
    def __init__(self):
        self.api = AccountAPI()
        self.trade = TradeAPI()
        self.dataokex = ConnectOkex()
        self.database = ConnectData()
        self.combo = "okex_usd_swap-okex_usdt_swap"
        self.ccy = "BTC"
        self.interval = "1H"
        self.mv = {}
        self.exchange = "okex"
        self.slip_unit = 10000
        self.real_uswap = 0.0002
        self.real_cswap = 0
        self.fake_uswap = 0.00015
        self.fake_cswap = -0.0001
    
    def load_mongo_url(self) -> str:
        with open(f"{os.environ['HOME']}/.cryptobridge/private_key.yml") as f:
            data = yaml.load(f, Loader= yaml.SafeLoader)
        for info in data:
            if "mongo" in info:
                mongo_uri = info["mongo"]
                return mongo_uri
    
    def dt_to_ts(self, dt: datetime.datetime) -> int:
        return int(datetime.datetime.timestamp(dt.replace(tzinfo = None).astimezone(pytz.timezone("Asia/ShangHai"))) * 1000)

    def ts_to_dt(self, ts) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(float(ts) / 1000).astimezone(pytz.timezone("Asia/ShangHai"))
    
    def get_long_bills(self, name: str, start: datetime.datetime, end: datetime.datetime) -> pd.DataFrame:
        self.api.name = name
        self.api.load_account_api()
        start = self.dt_to_ts(start)
        ts = self.dt_to_ts(end)
        data = []
        while ts >= start:
            response = self.api.get_bills_details(end = ts)
            ret = response.json()["data"] if response.status_code == 200 else {"data": []}
            data += ret
            ts = int(ret[-1]["ts"]) if len(ret) > 0 else ts
            if len(ret) <= 1:
                break
        df = pd.DataFrame(data)
        return df.drop_duplicates()
    
    def handle_bills(self, data: pd.DataFrame, is_play = True) -> pd.DataFrame:
        data = data[((data["type"] == "2") | (data["type"] == "8")) & (data["instType"] == "SWAP")].copy()
        data["dt"] = data["ts"].apply(lambda x: self.ts_to_dt(x))
        data["coin"] = data["instId"].apply(lambda x: x.split("-")[0])
        data.sort_values(by = "ts", inplace=True)
        data.index = range(len(data))
        data[["bal", "balChg", "fee", "pnl", "px", "sz"]] = data[["bal", "balChg", "fee", "pnl", "px", "sz"]].astype(float)
        for i in data[data["type"] == "2"].index:
            if data.loc[i, "ccy"] in ["USDT", "USD", "USDK", "BUSD", "USDC"]:
                data.loc[i, "balChg_U"] = data.loc[i, "balChg"]
                data.loc[i, "fake_fee"] = - data.loc[i, "sz"] * self.dataokex.get_contractsize_uswap(data.loc[i, "coin"]) * data.loc[i, "px"] * self.fake_uswap
                data.loc[i, "fake_balChg_U"] = data.loc[i, "pnl"] + data.loc[i, "fake_fee"]
            else:
                data.loc[i, "balChg_U"] = data.loc[i, "balChg"] * data.loc[i, "px"]
                data.loc[i, "fake_fee"] = - data.loc[i, "sz"] * self.dataokex.get_contractsize_cswap(data.loc[i, "coin"]) * self.fake_cswap
                data.loc[i, "fake_balChg_U"] = data.loc[i, "pnl"] * data.loc[i, "px"] + data.loc[i, "fake_fee"]
        data["cum_pnl"] = data["balChg_U"].cumsum()
        data["fake_cum_pnl"] = data["fake_balChg_U"].cumsum()
        if is_play:
            result = data[["dt", "cum_pnl", "fake_cum_pnl"]].copy()
            result.set_index("dt",inplace=True)
            draw_ssh.line(result)
        self.bills_data = data
        return data
    
    def get_pnl(self, name: str, start: datetime.datetime, end: datetime.datetime, is_play = True) -> pd.DataFrame:
        df = self.get_long_bills(name, start, end)
        ret = self.handle_bills(df, is_play)
    
    def get_slip(self, name: str, start: datetime.datetime, end: datetime.datetime, is_play = True) -> pd.DataFrame:
        start -= datetime.timedelta(hours = 8)
        end -= datetime.timedelta(hours = 8)
        client, username = name.split("_")
        sql = f"""
        SELECT cum_deal_base, exp_price, avg_price, hint, pair, real_spread, side, slip_page FROM "log_slip_page" WHERE time > '{start}' and time < '{end}' and username = '{username}' and client = '{client}'
        """
        ret = self.database._send_influx_query(sql, database= "account_data")
        ret["turnover"] = ret["cum_deal_base"] * ret["avg_price"]
        ret["slip"] = ret["turnover"] * ret["slip_page"]
        slip = pd.DataFrame(columns = ["turnover", "slip", "slip_page", "dt"])
        n = 1
        turnover, cum_slip = 0, 0
        for i in ret.index:
            turnover += ret.loc[i, "turnover"]
            cum_slip += ret.loc[i, "slip"]
            if turnover >= self.slip_unit:
                cum_slip -= ret.loc[i, "slip"] * (1-self.slip_unit / turnover)
                slip.loc[n] = [n * self.slip_unit, cum_slip, cum_slip / self.slip_unit, datetime.datetime.strptime(ret.loc[i, "time"][:19], "%Y-%m-%dT%H:%M:%S")]
                n += 1
                cum_slip = ret.loc[i, "slip"] * (1-self.slip_unit / turnover)
                turnover -= self.slip_unit
        slip.loc[n] = [turnover + (n-1) * self.slip_unit, cum_slip, cum_slip / turnover, datetime.datetime.strptime(ret.loc[i, "time"][:19], "%Y-%m-%dT%H:%M:%S")]
        if is_play:
            result = slip[["turnover", "slip_page"]].copy()
            result.set_index("turnover", inplace=True)
            p = draw_ssh.line(result, x_axis_type="linear", play=False)
            p.yaxis[0].formatter = NumeralTickFormatter(format="0.0000%")
            p.xaxis[0].formatter = NumeralTickFormatter(format="0,0")
            show(p)
            result = slip[["dt", "slip_page"]].copy()
            result.set_index("dt", inplace=True)
            p = draw_ssh.line(result, play=False)
            p.yaxis[0].formatter = NumeralTickFormatter(format="0.0000%")
            show(p)
        self.slip = slip
        return slip
    
    def get_deal(self, deploy_id: str, start: datetime.datetime, end: datetime.datetime) -> dict:
        account = AccountOkex(deploy_id)
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
        self.trade.name = name
        self.trade.load_account_api()
        start = self.dt_to_ts(start)
        ts = self.dt_to_ts(end)
        data = []
        while ts >= start:
            response = self.trade.orders_history_archive(instType="SPOT", end = ts)
            ret = response.json()["data"] if response.status_code == 200 else {"data": []}
            data += ret
            ts = int(ret[-1]["uTime"]) if len(ret) > 0 else ts
            if len(ret) <= 1:
                break
        df = pd.DataFrame(data).drop_duplicates()
        df["dt"] = df["uTime"].apply(lambda x: datetime.datetime.fromtimestamp(float(x)/1000))
        df["coin"] = df["instId"].apply(lambda x: x.split("-")[0].upper())
        df["is_maker"] = True
        df["is_trade"] = df["state"] != "canceled"
        self.is_maker, self.is_trade = {}, {}
        self.order = {}
        for coin in df["coin"].unique():
            self.order[coin] = pd.DataFrame(columns = ["is_maker", "is_trade"])
            self.is_maker[coin] = df[df["coin"] == coin].set_index("dt")[["is_maker"]].resample("1Min").sum()
            self.is_trade[coin] = df[df["coin"] == coin].set_index("dt")[["is_trade"]].resample("1Min").sum()
            self.order[coin]["is_maker"] = self.is_maker[coin]["is_maker"].apply(lambda x: True if x > 0 else False)
            self.order[coin]["is_trade"] = self.is_trade[coin]["is_trade"].apply(lambda x: True if x > 0 else False)
        return df

    def get_mv(self, coin: str, name: str,start: datetime.datetime, end: datetime.datetime) -> None:
        client, username = name.split("_")
        a = f"""
        SELECT mean(long) + mean(short) as mv FROM "position" WHERE time > '{start + datetime.timedelta(hours = -8)}' and time < '{end + datetime.timedelta(hours = -8)}' 
        and username = '{username}' and client = '{client}' and pair = '{coin.lower()}-usd-swap' GROUP BY time({self.interval.lower()})
        """
        position = self.database._send_influx_query(sql = a, database = "account_data")
        a = f"""
        SELECT mean(usdt) as adjEq FROM "balance_v2" WHERE time > '{start + datetime.timedelta(hours = -8)}' and time < '{end + datetime.timedelta(hours = -8)}'
        and username = '{username}' and client = '{client}' and balance_id != None GROUP BY time({self.interval.lower()}) 
        """
        equity = self.database._send_influx_query(sql = a, database = "account_data")
        result = pd.merge(position, equity, on = "time", how="outer").fillna(method="ffill")
        result["dt"] = result["time"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ") + datetime.timedelta(hours = 8))
        result.set_index("dt", inplace=True)
        result["mv"] = result["mv"] * 10 if coin.upper() != "BTC" else result["mv"] * 100
        result["mv%"] = result["mv"] / result["adjEq"]
        return result
    
    def get_rate(self, deploy_id: str, start: datetime.datetime, end: datetime.datetime, is_play = True) -> dict:
        self.rate = {}
        self.count = {}
        self.get_deal(deploy_id, start, end)
        self.get_orders(deploy_id.split("@")[0], start, end)
        for coin in set(self.is_reach.keys()) & set(self.order.keys()):
            self.count[coin] = pd.merge(self.is_reach[coin], self.order[coin], left_index= True, right_index= True)
            self.rate[coin] = self.count[coin].resample(self.interval).mean()
            self.mv[coin] = self.get_mv(coin, deploy_id.split("@")[0], start, end)
            self.rate[coin] = pd.merge(self.rate[coin], self.mv[coin][["mv%"]], left_index=True, right_index=True)
        if is_play:
            tabs = []
            for coin in self.rate.keys():
                p = draw_ssh.line_doubleY(self.rate[coin], right_columns=["mv%"], title = deploy_id.split("@")[0], play = False)
                p.yaxis[1].formatter = NumeralTickFormatter(format="0.0000%")
                tab = Panel(child = p, title = coin)
                tabs.append(tab)
            t = Tabs(tabs = tabs)
            show(t)
        return self.rate