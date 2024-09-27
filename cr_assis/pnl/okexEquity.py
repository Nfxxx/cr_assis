import datetime, pytz
from cr_assis.connect.connectData import ConnectData
import pandas as pd
from cr_assis.pnl.okexPnl import OkexPnl
from cr_assis.draw import draw_ssh
from bokeh.plotting import show
from bokeh.models import LinearAxis, Range1d

class OkexEquity(object):
    
    def __init__(self) -> None:
        self.tz = "Asia/ShangHai"
        self.pnl = OkexPnl()
        self.database = ConnectData()
    
    def get_account_info(self) -> None:
        self.balance_id = self.deploy_id.replace("@", "-") + "@sum"
        self.name = self.deploy_id.split("@")[0]
        self.client, self.username = self.name.split("_")
        self.sql_start = str(self.start.astimezone(pytz.timezone("UTC"))).split("+")[0]
        self.sql_end = str(self.end.astimezone(pytz.timezone("UTC"))).split("+")[0]
    
    def get_position(self) -> pd.DataFrame:
        a = f"""
        select (long-short) * 100 as btc_position from "position" where time > '{self.sql_start}' and time < '{self.sql_end}' 
        and username = '{self.username}' and client = '{self.client}' and pair = 'btc-usd-swap'
        """
        position = self.database._send_influx_query(a, database = "account_data")
        position.index = position["time"].apply(lambda x: datetime.datetime.strptime(x[:19], "%Y-%m-%dT%H:%M:%S").astimezone(pytz.timezone("Asia/ShangHai")))
        position.index = position.index + datetime.timedelta(hours = 8)
        return position
    
    def get_equity(self) -> pd.DataFrame:
        a = f"""
        select btc as equity, usdt from "balance_v2" where time > '{self.sql_start}' and time < '{self.sql_end}' and balance_id = '{self.balance_id}'
        """
        data = self.database._send_influx_query(a, database = "account_data")
        data.index = data["time"].apply(lambda x: datetime.datetime.strptime(x[:19], "%Y-%m-%dT%H:%M:%S").astimezone(pytz.timezone("Asia/ShangHai")))
        data.index = data.index + datetime.timedelta(hours = 8)
        data["price"] = data["usdt"] / data["equity"]
        return data
    
    def get_pnl(self) -> pd.DataFrame:
        result = self.pnl.get_long_bills(name = self.name, start = self.start, end = self.end)
        ret = self.pnl.handle_bills(result, is_play=False)
        return ret[(ret["dt"] >= self.start) & (ret["dt"] <= self.end)].copy()
    
    def run_equity(self, deploy_id: str, start: datetime.datetime, end: datetime.datetime, is_play = True, plot_width = 900, plot_height = 600,) -> pd.DataFrame:
        self.deploy_id, self.start, self.end = deploy_id, start.replace(tzinfo=pytz.timezone("Asia/ShangHai")), end.replace(tzinfo=pytz.timezone("Asia/ShangHai"))
        self.get_account_info()
        position = self.get_position()
        equity = self.get_equity()
        position = pd.merge(position, equity[["usdt"]], right_index=True, left_index=True, how="outer").fillna(method="ffill").dropna(how = "any")
        position["mv%"] = position["btc_position"] / position["usdt"]
        ret = self.get_pnl()
        result = pd.merge(ret.set_index("dt")[["cum_pnl", "fake_cum_pnl"]], equity, right_index = True, left_index = True, how = "outer").fillna(method="ffill")
        result[["cum_pnl", "fake_cum_pnl"]] = result[["cum_pnl", "fake_cum_pnl"]].fillna(0)
        result["fake_equity"] = result["equity"] + (result["fake_cum_pnl"] - result["cum_pnl"]) / result["price"]
        p1 = draw_ssh.line(result[["equity", "fake_equity"]].dropna(how = "any"), play = False, plot_width = plot_width, plot_height = plot_height)
        y_column2_range = 'settle_range'
        p1.extra_y_ranges = {y_column2_range:Range1d(start = float(min(position["mv%"])), end = float(max(position["mv%"])))}
        p1.add_layout(LinearAxis(y_range_name = y_column2_range),'right')
        p1.varea(position.index,y1=0, y2 = position["mv%"], alpha=0.1, y_range_name=y_column2_range,color = '#55FF88', name = "mv%", legend_label="mv%")
        show(p1) if is_play else None