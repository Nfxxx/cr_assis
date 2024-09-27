from .okexWallet import OkexWallet
from cr_assis.api.gate.accountApi import AccountAPI
import os, yaml

class GateWallet(OkexWallet):
    
    def __init__(self) -> None:
        self.account_api = AccountAPI()
        self.file_path = "/mnt/efs/fs1/data_ssh/mm/gate/total" if os.path.exists("/mnt/efs/fs1/data_ssh/mm/gate/total") else os.environ["HOME"] + "/data/mm/gate/total"
        self.futures_usdt: dict[str, dict] = {}
    
    def get_accounts(self) -> None:
        with open(f"{os.environ['HOME']}/.cr_assis/account_gate_api.yml", "rb") as f:
            data: list[dict] = yaml.load(f, Loader= yaml.SafeLoader)
        self.accounts = [i['name'] for i in data if "hf" == i['name'].split("_")[0]]
    
    def get_futures_usdt_info(self) -> dict[str, str]:
        response = self.account_api.get_futures_usdt_balance()
        self.futures_usdt[self.account_api.name] =response.json() if response.status_code == 200 else {"total": "nan", "unrealised_pnl": "nan", "position_margin":"nan"}
        return self.futures_usdt[self.account_api.name]
    
    def get_wallet_equity(self) -> float:
        info = self.futures_usdt[self.account_api.name] if hasattr(self, "futures_usdt") and self.account_api.name in self.futures_usdt else self.get_futures_usdt_info()
        return float(info["total"]) + float(info["unrealised_pnl"])
    
    def get_wallet_position(self) -> float:
        info = self.futures_usdt[self.account_api.name] if hasattr(self, "futures_usdt") and self.account_api.name in self.futures_usdt else self.get_futures_usdt_info()
        return float(info["position_margin"])