import research.eva.eva as eva
import sys, os, datetime, yaml
import numpy as np
with open(f"{os.environ['HOME']}/.cryptobridge/private_key.yml", "rb") as f:
    data = yaml.load(f, Loader= yaml.SafeLoader)
for info in data:
    if "mongo" in info.keys():
        os.environ["MONGO_URI"] = info['mongo']
        os.environ["INFLUX_URI"] = info['influx']
        os.environ["INFLUX_MARKET_URI"] = info['influx_market']
import pandas as pd
class RunEva(object):
    def __init__(self):
        self.eva = eva
    
    def daily_get_funding(self, file_path = f"/mnt/efs/fs1/data_ssh/eva_result"):
        all_datas = self.eva.daily_get_funding(save = False)
        
        sheet_names = list(all_datas.keys())
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        writer = pd.ExcelWriter(f"{file_path}/" + str(datetime.date.today()) + '.xlsx', engine='openpyxl')
        for sheet_name in sheet_names:
            data = all_datas[sheet_name][0].copy()
            if "volume_U_24h" in data.columns:
                data["volume_U_24h"] = data["volume_U_24h"].apply(lambda x: str(x) if np.isnan(x) else format(int(x), ","))
            data.to_excel(excel_writer=writer, sheet_name=sheet_name, encoding="UTF-8")
        writer.save()
        writer.close()

def main():
    Eva = RunEva()
    Eva.daily_get_funding(file_path = "/mnt/efs/fs1/data_ssh/eva_result")

main()