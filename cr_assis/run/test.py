import datetime, time
import pandas as pd
from cr_assis.connect.connectOkex import ConnectOkex
from cr_assis.account.accountOkex import AccountOkex
from cr_assis.connect.connectOkex import ConnectOkex
from cr_assis.api.okex.accountApi import AccountAPI
from cr_assis.account.accountBinance import AccountBinance
from urllib.parse import urljoin, urlencode
import requests, json, time, hmac, hashlib

api = AccountAPI()
api.name = "test_hfok01"
api.load_account_api()
# response = api.get_order(instId = "LTC-USDT-SWAP", ordId="611342988853678083")
# response = api.get_order_history(instType="SWAP", instId="LTC-USDT-SWAP")
begin = int(datetime.datetime.timestamp(datetime.datetime(2023,8,16,3,0,0)) * 1000)
end = int(datetime.datetime.timestamp(datetime.datetime(2023,8,16,3,30,0)) * 1000)
ts = end
data = []
while ts > begin:
    response = api.get_bills_details(end = ts, limit=100)
    if response.status_code == 200:
        ret = response.json()["data"]
        data = data + ret
        ts = int(ret[-1]['ts'])
    else:
        break
df = pd.DataFrame(data)
cols = ["bal", "balChg", "fee", "pnl", "px", "sz", "ts"]
df.loc[df["px"] == "", "px"] = "nan"
df[cols] = df[cols].astype(float)
df["dt"] = df["ts"].apply(lambda x: datetime.datetime.fromtimestamp(float(x) / 1000))
df["balPx"] = df.apply(lambda x: x["px"] if x["ccy"] not in ["USDT", "USD", "USDC"] else 1, axis = 1)
df["balChgU"] = df["balChg"] * df["balPx"]


apikey = ""
secret = ""
servertime = requests.get("https://api.binance.com/api/v1/time")
BASE_URL = "https://papi.binance.com"
headers = {
    'X-MBX-APIKEY': apikey
}
servertimeobject = json.loads(servertime.text)
servertimeint = servertimeobject['serverTime']
PATH = '/papi/v1/cm/userTrades'
timestamp = int(time.time() * 1000)
start_time = int(datetime.datetime.timestamp(datetime.datetime(2023,8,10,0,0,0)) * 1000)
end_time = int(datetime.datetime.timestamp(datetime.datetime(2023,8,11,0,0,0)) * 1000)
params = {
    "timestamp": timestamp,
    "startTime":start_time,
    "endTime":end_time,
    "symbol": "BTCUSD_PERP",
    "limit": 1000
}
query_string = urlencode(params)
params['signature'] = hmac.new(secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
url = urljoin(BASE_URL, PATH)
r = requests.get(url, headers=headers, params=params)