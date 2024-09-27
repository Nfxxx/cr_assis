from cr_monitor.daily.dailyOkex import DailyOkex
from cr_assis.load import *
import datetime

daily = DailyOkex(ignore_test = True)
ts = (datetime.datetime.utcnow() + datetime.timedelta(hours = 8)).strftime("%Y-%m-%d %H:%M:%S")
daily.mr_okex.price_range = [1]
result = daily.get_account_mr()
data = pd.DataFrame.from_dict(daily.account_mr)
data.loc[[1]].rename(index = {1: ts}).to_csv("/mnt/efs/fs1/data_ssh/cal_mr/okex6.csv", mode = "a", header= (not os.path.isfile(f"/mnt/efs/fs1/data_ssh/cal_mr/okex6.csv")))