import requests, datetime, copy, pytz
import pandas as pd

def req_okex_index(instId: str, end_ts: int, start_ts = None) -> list:
    if start_ts != None:
        url = f"https://www.okx.com/api/v5/market/history-index-candles?instId={instId}&after={end_ts}&before={start_ts}"
    else:
        url = f"https://www.okx.com/api/v5/market/history-index-candles?instId={instId}&after={end_ts}"
    ret = requests.get(url)
    data = ret.json()['data']
    return data

def handle_origin_index(data: list) -> pd.DataFrame:
    spot_index = pd.DataFrame(columns = ["timestamp", "open", "high", "low", "close"])
    for i in range(len(data)):
        info = data[i]
        ts, op, high, low, close, is_update = info
        if is_update == "1":
            timestamp = datetime.datetime.fromtimestamp(int(ts) / 1000, tz = datetime.timezone.utc)
            spot_index.loc[i] = [timestamp, float(op), float(high), float(low), float(close)]
        else:
            pass
    spot_index["dt"] = spot_index["timestamp"].apply(lambda x: x.astimezone(pytz.timezone("Asia/ShangHai")))
    return spot_index

def get_okex_index(coin: str, start: datetime.datetime, end: datetime.datetime, is_usdt = True) -> pd.DataFrame:
    """get spot index in okex
    Args:
        coin(str): spot coin name
        start(datetime.datetime): start utc time
        end(datetime.datetime): end utc time
        is_usdt: based on usdt or usd
    Returns:
        klines(pd.DataFrame): kline of spot index, incluing timestamp(UTC), open, high, low, close, dt(UTC+8)
    """
    
    interval = 60000
    start_ts = int(start.timestamp() * 1000)
    end_ts = int(end.timestamp() * 1000)
    if is_usdt:
        instId = coin.upper() + "-USDT"
    else:
        instId = coin.upper() + "-USD"
    ts = copy.deepcopy(end_ts)
    klines = pd.DataFrame()
    while ts >= start_ts:
        data = req_okex_index(instId, ts)
        spot_index = handle_origin_index(data)
        klines = pd.concat([klines, spot_index])
        ts = ts - interval * 100
    klines.sort_values(by = "timestamp", inplace = True)
    klines.drop_duplicates(subset = ["timestamp"], inplace = True)
    klines.index = range(len(klines))
    return klines

klines = get_okex_index(coin = "btc", start=datetime.datetime(2022,12,13,0,0,0), end = datetime.datetime.utcnow(), is_usdt=False)