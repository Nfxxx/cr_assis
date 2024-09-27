from .evaOkexWallet import EvaOkexWallet
import os, yaml

class EvaGateWallet(EvaOkexWallet):
    
    def __init__(self):
        self.file_path = "/mnt/efs/fs1/data_ssh/mm/gate/total" if os.path.exists("/mnt/efs/fs1/data_ssh/mm/gate/total") else os.environ["HOME"] + "/data/mm/gate/total"
        self.get_accounts()
    
    def get_accounts(self) -> None:
        with open(f"{os.environ['HOME']}/.cr_assis/account_gate_api.yml", "rb") as f:
            data: list[dict] = yaml.load(f, Loader= yaml.SafeLoader)
        self.accounts = [i['name'] for i in data if "hf" == i['name'].split("_")[0]]