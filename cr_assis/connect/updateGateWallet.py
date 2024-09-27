import time, hashlib, hmac, requests, json, copy, datetime, os, yaml
import pandas as pd
import numpy as np

class UpdateGateWallet(object):
    
    def __init__(self, file_path = "/mnt/efs/fs1/data_ssh/mm/capital/gate"):
        self.file_path = file_path
        self.now = datetime.datetime.utcnow().replace(microsecond=0)
        self.load_key()
        self.query_ret: list[dict[str, str]] = []
        self.single_equity: dict[str, float] = {}
        self.single_mv: dict[str, float] = {}
        
    def load_key(self):
        with open(f"{self.file_path}/key.yml") as f:
            data = yaml.load(f, Loader= yaml.SafeLoader)
        self.api_key = data["api_key"]
        self.secret_key = data["secret_key"]

    def gen_sign(self, method, url, query_string=None, payload_string=None):
        key = self.api_key       # api_key
        secret = self.secret_key     # api_secret
        t = time.time()
        m = hashlib.sha512()
        m.update((payload_string or "").encode('utf-8'))
        hashed_payload = m.hexdigest()
        s = '%s\n%s\n%s\n%s\n%s' % (method, url, query_string or "", hashed_payload, t)
        sign = hmac.new(secret.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
        return {'KEY': key, 'Timestamp': str(t), 'SIGN': sign}
    
    def send_requests(self):
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        sign_headers = self.gen_sign('GET', "/api/v4/wallet/sub_account_futures_balances", '')
        headers.update(sign_headers)
        response = requests.request('GET', "https://api.gateio.ws/api/v4/wallet/sub_account_futures_balances", headers=headers)
        ret = response.json() if response.status_code == 200 else {}
        self.response = response
        self.query_ret = copy.deepcopy(ret)
        return ret
    
    def get_total_pnl(self):
        self.total_pnl, self.total_capital, self.total_equity,self.dnw_sum, self.total_position = 0, 0, 0, 0, 0
        for i in self.query_ret:
            info = i["available"]["usdt"]
            self.total_pnl += float(info["history"]["pnl"])
            self.single_equity[i["uid"]] = float(info["total"]) + float(info["unrealised_pnl"])
            self.total_capital += float(info["total"]) + float(info["unrealised_pnl"]) - float(info["history"]["dnw"])
            self.total_equity += self.single_equity[i["uid"]]
            self.dnw_sum += float(info["history"]["dnw"])
            self.total_position += float(info["position_margin"])
            self.single_mv[i["uid"]] = float(info["position_margin"]) / (float(info["total"]) + float(info["unrealised_pnl"])) if float(info["total"]) + float(info["unrealised_pnl"]) != 0 else 0
        self.total_mv = self.total_position / self.total_capital if self.total_capital != 0 else np.nan
        data = pd.DataFrame(columns = ["total_pnl", "total_capital", "total_equity","dnw_sum", "total_mv"])
        data.loc[self.now] = [self.total_pnl, self.total_capital, self.total_equity,self.dnw_sum, self.total_mv]
        self.total_summary = data
    
    def save_total_data(self):
        self.get_total_pnl() if not hasattr(self, "total_pnl") else None
        self.total_summary.to_csv(f"{self.file_path}/total/{self.now.date()}.csv", mode = "a", header= (not os.path.isfile(f"{self.file_path}/total/{self.now.date()}.csv")))
    
    def save_single_data(self):
        self.get_total_pnl() if not hasattr(self, "total_pnl") else None
        pd.DataFrame(self.single_equity, index = [self.now]).to_csv(f"{self.file_path}/subaccount/equity/{self.now.date()}.csv", mode = "a", header=(not os.path.isfile(f"{self.file_path}/subaccount/equity/{self.now.date()}.csv")))
        pd.DataFrame(self.single_mv, index = [self.now]).to_csv(f"{self.file_path}/subaccount/mv/{self.now.date()}.csv", mode = "a", header=(not os.path.isfile(f"{self.file_path}/subaccount/mv/{self.now.date()}.csv")))
    
    def handle_ret(self):
        self.send_requests()
        self.get_total_pnl() if self.query_ret != {} else None
        self.save_total_data() if self.query_ret != {} else None
        self.save_single_data() if self.query_ret != {} else None