import pandas as pd
from cr_assis.connect.connectData import ConnectData
from IPython.display import display

database = ConnectData()
database.load_influxdb()
a = f"""
select * from "order_arb_bp" WHERE time > '2023-01-03 12:00:00' 
"""
ret = database.influx_clt.query(a)
data = pd.DataFrame(ret.get_points())
database.influx_clt.close()
result = pd.DataFrame(columns = ['time', "account_id", "price_cex", "price_dex", "side", "tx_hash", "cum_deal_base", "gas", "trade_fee", "pnl"])
batches = data["batch_id"].unique()
n = 0
for batch in batches:
    df = data[data["batch_id"] == batch].copy()
    if "1" not in df["price"].values:
        result.loc[n, "time"] = df['time'].values[0]
        result.loc[n, "account_id"] = df["account_id"].values[0]
        result.loc[n, "price_cex"] = float(df[df["exchange"] == "binance"]["price"].values[0])
        result.loc[n, "price_dex"] = float(df[df["exchange"] == "bsc-pancakeswap"]["price"].values[0])
        result.loc[n, "side"] = df[df["exchange"] == "binance"]["side"].values[0]
        result.loc[n, "tx_hash"] = df[df["exchange"] == "bsc-pancakeswap"]["client_oid"].values[0]
        result.loc[n, "cum_deal_base"] = float(df['cum_deal_base'].values[0])
        n += 1
result["gas"] = 0.16
result['trade_fee'] = result["price_cex"] * result["cum_deal_base"] * 0.0003 + result["price_dex"] * result["cum_deal_base"] * 0.0025
for i in result.index:
    if result.loc[i, "side"] == "sell":
        result.loc[i, "pnl"] = (result.loc[i, "price_cex"] - result.loc[i, "price_dex"]) * result.loc[i, "cum_deal_base"] - result.loc[i, "gas"] - result.loc[i, "trade_fee"]
profit = len(result[result['pnl']>0])
print(f"总收益：{sum(result['pnl'].values)}")
print(f"总次数：{len(result)}")
print(f"盈利次数：{profit}")
print(f"盈利率：{profit / len(result)}")
display(result)
print(11111)