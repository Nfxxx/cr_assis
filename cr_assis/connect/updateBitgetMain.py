from cr_assis.api.bitget.bitget.mix.account_api import AccountApi
from cr_assis.connect.updateGateWallet import UpdateGateWallet
import yaml
import pandas as pd
import numpy as np

class UpdateBitgetMain(UpdateGateWallet):
    
    def __init__(self, file_path="/mnt/efs/fs1/data_ssh/mm/capital/bitget"):
        super().__init__(file_path)
        self.account_api = AccountApi(self.api_key, self.secret_key, self.passphrase)
    
    def load_key(self):
        with open(f"{self.file_path}/key.yml") as f:
            data = yaml.load(f, Loader= yaml.SafeLoader)
        self.api_key = data["api_key"]
        self.secret_key = data["secret_key"]
        self.passphrase = data["passphrase"]
    
    def send_requests(self):
        self.query_ret = self.account_api.sub_account(productTye="umcbl")["data"]
    
    def get_total_pnl(self):
        self.total_equity, self.total_position = 0, 0
        for sub_assets in self.query_ret:
            for assets in sub_assets["contractAssetsList"]:
                self.total_equity += float(assets["usdtEquity"]) if assets["usdtEquity"] != "" else 0
                self.total_position += float(assets["locked"]) if assets["locked"] != "" else 0
        self.total_mv = self.total_position / self.total_equity if self.total_equity != 0 else np.nan
        data = pd.DataFrame(columns = ["total_equity", "total_mv"])
        data.loc[self.now] = [self.total_equity, self.total_mv]
        self.total_summary = data
    
    def handle_ret(self):
        self.send_requests()
        self.get_total_pnl() if self.query_ret != {} else None
        self.save_total_data() if self.query_ret != {} else None