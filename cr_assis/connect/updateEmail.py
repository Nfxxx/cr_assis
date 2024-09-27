from cr_assis.connect.connectData import ConnectData
import os, datetime
import pandas as pd
import numpy as np
from imap_tools import MailMessage, MailAttachment


class UpdateEmail(object):
    
    def __init__(self) -> None:
        self.database = ConnectData()
        self.database.load_mailbox()
        self.save_path = os.environ["HOME"] + "/data/account_volume/okex"
        self.start_date: datetime.date = (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).date() + datetime.timedelta(days= -2)
        self.end_date: datetime.date = (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).date() + datetime.timedelta(days= -1)
    
    def tell_email(self, msg: MailMessage) -> dict[MailAttachment, datetime.date]:
        download_atts = {}
        if msg.from_ == "coinrising111@outlook.com" and len(msg.attachments) >0:
            for att in msg.attachments:
                if att.filename[-4:] == ".csv":
                    cal_date = datetime.datetime.strptime(att.filename.split("-")[-1].split(".")[0], "%d%b%Y").date()
                    if cal_date >= self.start_date and cal_date <= self.end_date:
                        download_atts[att] = cal_date
                        os.makedirs(f"{self.save_path}/origin/{cal_date}") if not os.path.exists(f"{self.save_path}/origin/{cal_date}") else None
        return download_atts
    
    def download_volume_csv(self):
        """download origin csv file
        """
        for msg in self.database.mailbox.fetch():
            download_atts = self.tell_email(msg)
            for att, cal_date in download_atts.items():
                with open(f'{self.save_path}/origin/{cal_date}/{att.filename}', 'wb') as f:
                    f.write(att.payload)
    
    def read_now_data(self) -> datetime.date:
        """read saved csv file, return the last timestamp
        """
        date = self.start_date + datetime.timedelta(days = -1)
        data = self.read_special_csv(f"{self.save_path}/result/volume.csv")
        if len(data) > 0:
            dates = list(data.index.values)
            dates.sort()
            date = datetime.datetime.strptime(dates[-1], "%Y-%m-%d").date()
        self.saved_data: pd.DataFrame = data
        return date
    
    def read_special_csv(self, file_path: str) -> pd.DataFrame:
        """read special csv file, if not exists return empty DataFrame
        """
        data = pd.DataFrame()
        if os.path.isfile(file_path):
            data = pd.read_csv(file_path, index_col=0)
        return data
    
    def tell_file(self, path: str,name: str) -> str:
        """tell this file name under special file path
        """
        file_name = ""
        for file in os.listdir(path):
            if name in file:
                file_name = file
                break
        return file_name
    
    def get_daily_spot(self, date: datetime.date) -> float:
        """get this day's spot volume
        """
        file_name = self.tell_file(path = f"{self.save_path}/origin/{date}",name = "Spot")
        data = self.read_special_csv(file_path = f"{self.save_path}/origin/{date}/{file_name}")
        daily_volume = data["24h Total Vol."].sum() if "24h Total Vol." in data.columns else np.nan
        return daily_volume
    
    def get_daily_swap(self, date: datetime.date) -> float:
        """get this day's futures and swap volume
        """
        daily_volume = 0
        for name in ["USDs-Margined", "Crypto-Margined"]:
            file_name = self.tell_file(path = f"{self.save_path}/origin/{date}",name = name)
            data = self.read_special_csv(file_path = f"{self.save_path}/origin/{date}/{file_name}")
            daily_volume += data["24h Total Vol."].sum() if "24h Total Vol." in data.columns else np.nan
        return daily_volume
    
    def get_daily_option(self, date: datetime.date) -> float:
        """get this day's option volume
        """
        file_name = self.tell_file(path = f"{self.save_path}/origin/{date}",name = "Option")
        data = self.read_special_csv(file_path = f"{self.save_path}/origin/{date}/{file_name}")
        daily_volume = data["24h Total Vol."].sum() if "24h Total Vol." in data.columns else np.nan
        return daily_volume
    
    def get_daily_volume(self, date: datetime.date) -> pd.DataFrame:
        """read origin csv and get this day's account volume

        Args:
            date (datetime.date): the day 

        Returns:
            pd.DataFrame: columns = ["spot", "future_perp", "option"], index = list[datetime.date]
        """
        spot_volume, swap_volume, option_volume = self.get_daily_spot(date), self.get_daily_swap(date), self.get_daily_option(date)
        volume = pd.DataFrame(columns = ["spot", "future_perp", "option"])
        volume.loc[str(date)] = [spot_volume, swap_volume, option_volume]
        return volume
    
    def update_account_volume(self):
        """main function, update daily account volume csv
        """
        date = self.read_now_data() + datetime.timedelta(days = 1)
        self.start_date = date
        self.download_volume_csv() if date <= self.end_date else None
        while date <= self.end_date:
            ret = self.get_daily_volume(date)
            self.saved_data = pd.concat([self.saved_data, ret])
            date += datetime.timedelta(days = 1)
        self.saved_data.drop_duplicates(inplace= True)
        self.saved_data.sort_index(inplace=True)
        os.mkdir(f"{self.save_path}/result") if not os.path.exists(f"{self.save_path}/result") else None
        self.saved_data.to_csv(f"{self.save_path}/result/volume.csv")