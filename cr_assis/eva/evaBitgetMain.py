import datetime
from cr_assis.eva.evaGateWallet import EvaGateWallet
from cr_assis.load import *
from cr_assis.load import datetime

class EvaBitGetMain(EvaGateWallet):
    def __init__(self):
        super().__init__()
        self.file_path = "/mnt/efs/fs1/data_ssh/mm/capital/bitget"
    
