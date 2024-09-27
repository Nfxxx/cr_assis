from .evaGateWallet import EvaGateWallet
import os, datetime, yaml, copy
from cr_assis.load import *
from cr_assis.draw import draw_ssh
from bokeh.plotting import figure,show
from bokeh.models import NumeralTickFormatter
from bokeh.models.widgets import Panel, Tabs

class EvaOkexWallet(EvaGateWallet):
    
    def __init__(self):
        self.file_path = "/mnt/efs/fs1/data_ssh/mm/okex/total" if os.path.exists("/mnt/efs/fs1/data_ssh/mm/okex/total") else os.environ["HOME"] + "/data/mm/okex/total"
        self.get_accounts()
    
    def get_accounts(self) -> None:
        with open(f"{os.environ['HOME']}/.cr_assis/account_okex_api.yml", "rb") as f:
            data: list[dict] = yaml.load(f, Loader= yaml.SafeLoader)
        self.accounts = [i['name'] for i in data if "hf" == i['name'].split("_")[0]]
    
    def read_total_summary(self, start: datetime.datetime, end: datetime.datetime, accounts = [],is_play = True):
        accounts = self.accounts if accounts == [] else accounts
        tabs = []
        self.origin_total = {}
        for account in accounts:
            total_summary = self.read_data(path = f"{self.file_path}/{account}", start = start, end = end)
            self.total_summary = total_summary.drop("position_value", axis = 1) if "position_value" in total_summary.columns else total_summary
            kline = self.get_btc_price(start, end)
            kline.set_index("dt", inplace = True)
            self.origin_total[account] = pd.merge(self.total_summary, kline[["open"]].astype(float), left_index=True, right_index=True, how = "outer").fillna(method = "ffill")
            if is_play:
                p = draw_ssh.line_triY(self.origin_total[account],left_columns = ["equity"], play = False)
                p.yaxis[0].formatter = NumeralTickFormatter(format="0,0")
                p.yaxis[1].formatter = NumeralTickFormatter(format="0.0000%")
                p.yaxis[2].formatter = NumeralTickFormatter(format="0.00")
            if p:
                tab = Panel(child = p, title = account)
                tabs.append(tab)
        if len(tabs) > 0:
            t = Tabs(tabs = tabs)
            show(t)
