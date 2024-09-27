from pymongo import MongoClient
from influxdb import InfluxDBClient
from imap_tools import MailBox
import pandas as pd
import numpy as np
import redis, os, yaml, requests
import urllib3
urllib3.disable_warnings()

class ConnectData(object):
    def __init__(self):
        self.mongo_uri = self.load_mongo_uri()
        self.mongo_clt = MongoClient(self.mongo_uri)
        self.influx_json = None
        self.influx_clt = None
    
    def load_mongo_uri(self):
        with open(f"{os.environ['HOME']}/.cryptobridge/private_key.yml") as f:
            data = yaml.load(f, Loader= yaml.SafeLoader)
        for info in data:
            if "mongo" in info:
                mongo_uri = info["mongo"]
                return mongo_uri
    
    def load_email_account(self):
        with open(f"{os.environ['HOME']}/.cr_assis/mongo_url.yml", "rb") as f:
            data = yaml.load(f, Loader= yaml.SafeLoader)
        for info in data:
            if "name" in info.keys() and info["name"] == "gmail":
                self.email_address: str = info["address"]
                self.email_password: str = info["password"]
    
    def load_influxdb(self, database = "ephemeral"):
        db = "DataSource"
        coll = "influx"
        influx_json = self.mongo_clt[db][coll].find_one({"_id" : {"$regex" : f".*{database}$"}})
        client = InfluxDBClient(host = influx_json["host"],
                port = influx_json["port"],
                username = influx_json["username"],
                password = influx_json["password"],
                database = influx_json["database"],
                ssl = influx_json["ssl"])
        self.influx_json = influx_json
        self.influx_clt = client
    
    def load_redis(self, database = "dratelimit_new"):
        db = "DataSource"
        coll = "redis"
        redis_json = self.mongo_clt[db][coll].find_one({"_id" : {"$regex" : f".*{database}$"}})
        pool = redis.ConnectionPool(host = redis_json["host"], password = redis_json["password"])
        self.redis_clt = redis.Redis(connection_pool = pool)
        self.redis_json = redis_json
        
    def get_redis_data(self, key: str):
        self.load_redis() if not hasattr(self, "redis_clt") else None
        key = bytes(key, encoding = "utf8")
        data = self.redis_clt.hgetall(key) if key in self.redis_clt.keys() else {}
        return data
    
    def get_redis_okex_price(self, coin: str, suffix: str) -> float:
        self.load_redis() if not hasattr(self, "redis_clt") else None
        key = bytes(f"okexv5/{coin.lower()}-{suffix}", encoding="utf8")
        price = float(self.redis_clt.hgetall(key)[b'bid0_price']) if key in self.redis_clt.keys() else np.nan
        return price

    def _send_influx_query(self, sql: str, database: str, is_dataFrame = True):
        self.load_influxdb(database)
        resp = self.influx_clt.query(sql)
        self.influx_clt.close()
        if is_dataFrame:
            resp = pd.DataFrame(resp.get_points())
        return resp
    
    async def _send_pancake_query(self, sql: str):
        # url = "https://data-platform.nodereal.io/graph/v1/d45b5a4e909648b693abb623ba6b6c89/projects/pancakeswap"
        url = "https://proxy-worker.pancake-swap.workers.dev/bsc-exchange"
        headers = {
                "accept": "application/json",
                "content-type": "application/json"
            }
        payload = {"query": sql}
        response = requests.post(url, json=payload, headers=headers)
        return response
    
    async def get_pool_info(self, pool_id: str):
        sql =   """query pool {    
                pairs(        
                where: { id: "$pool_id" }\n   ) 
                {\n          
                reserve0\n   
                reserve1\n   
                reserveUSD\n  
                token0 {\n      id\n      symbol\n      name\n    decimals\n}\n    
                token1 {\n      id\n      symbol\n      name\n    decimals\n}\n         
                }}""".replace("$pool_id", pool_id)
        response = await self._send_pancake_query(sql = sql)
        if response.status_code == 200:
            ret = response.json()['data']
            data = ret['pairs'][0]
        else:
            data = {}
        return data

    async def get_pool_volume(self, pool_id: str):
        sql =   """query pool {    
                pairDayData(id: "$pool_id"\n   ) 
                {\n 
                dailyVolumeToken0\n
                dailyVolumeToken1\n
                dailyVolumeUSD\n
                }}""".replace("$pool_id", pool_id)
        response = await self._send_pancake_query(sql = sql)
        if response.status_code == 200:
            ret = response.json()['data']
            data = ret['pairDayData']
        else:
            data = {}
        return data
    
    def load_mailbox(self):
        self.load_email_account() if not hasattr(self, "email_password") else print(1111111)
        self.mailbox = MailBox("imap.gmail.com").login(self.email_address, self.email_password)