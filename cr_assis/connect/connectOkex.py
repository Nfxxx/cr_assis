import ccxt, requests, os, datetime, base64, hashlib, hmac
from cr_monitor.position.disacount_data import DisacountData
import pandas as pd
import numpy as np

class ConnectOkex(object):
    
    def __init__(self) -> None:
        self.markets = ccxt.okex().load_markets()
        self.discount_data = DisacountData()
        self.tiers_url:str  = 'https://www.okex.com/api/v5/public/position-tiers'
        self.discount_url:str  = "https://www.okex.com/api/v5/public/discount-rate-interest-free-quota"
        self.contractsize_path = f"{os.environ['HOME']}/parameters/config_buffet/dt/contractsize.csv"
        self.contractsize_uswap: dict[str, float] = {}
        self.contractsize_cswap: dict[str, float] = {}
        self.contractsize_usdc: dict[str, float] = {}
        self.discount_info: dict[str, list] = {}
        self.tier_spot: dict[str, pd.DataFrame] = {}
        self.tier_uswap: dict[str, pd.DataFrame] = {}
        self.tier_usdc: dict[str, pd.DataFrame] = {}
        self.tier_cswap: dict[str, pd.DataFrame] = {}
        self.tier_ufuture: dict[str, pd.DataFrame] = {}
        self.tier_cfuture: dict[str, pd.DataFrame] = {}
    
    def handle_account_get_query(self, query: str, secret: str, api_key: str, passphrase: str) -> requests.Response:
        timestamp = datetime.datetime.now().astimezone(datetime.timezone.utc).isoformat(timespec='milliseconds').replace("+00:00", "Z")
        message = timestamp + "GET" + query
        signature = base64.b64encode(hmac.new(bytes(secret, "utf-8"), bytes(message, "utf-8"), digestmod=hashlib.sha256).digest())
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "OK-ACCESS-KEY": api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": passphrase
        }
        url = f"https://www.okx.com{query}"
        return requests.get(url, headers=headers)
    
    def load_local_contractsize(self):
        data = pd.read_csv(self.contractsize_path, index_col= 0)
        self.contractsize_uswap = data["okex-usdt-swap"].to_dict()
        self.contractsize_cswap = data["okex-usd-swap"].to_dict()
        self.contractsize_usdc = data["okex-usdc-swap"].to_dict()
    
    def get_contractsize_uswap(self, coin: str) -> float:
        coin = coin.upper()
        contractsize = self.get_market_contractsize(f"{coin}/USDT:USDT") if coin not in self.contractsize_uswap.keys() else self.contractsize_uswap[coin]
        self.contractsize_uswap[coin] = contractsize
        return contractsize
    
    def get_contractsize_cswap(self, coin: str) -> float:
        coin = coin.upper()
        contractsize = self.get_market_contractsize(f"{coin}/USD:{coin}") if coin not in self.contractsize_cswap.keys() else self.contractsize_cswap[coin]
        self.contractsize_cswap[coin] = contractsize
        return contractsize
    
    def get_contractsize_usdc(self, coin: str) -> float:
        coin = coin.upper()
        contractsize = self.get_market_contractsize(f"{coin}/USDC:USDC") if coin not in self.contractsize_usdc.keys() else self.contractsize_usdc[coin]
        self.contractsize_usdc[coin] = contractsize
        return contractsize
    
    def get_market_contractsize(self, symbol: str) -> float:
        return self.markets[symbol]["contractSize"] if symbol in self.markets.keys() and "contractSize" in self.markets[symbol] else np.nan
    
    def get_discount_info(self, coin: str) -> list:
        ret = [] if coin not in self.discount_info.keys() else self.discount_info[coin]
        if ret != []:
            return ret
        for name in ["lv1", "lv2", "lv3", "lv4", "lv5", "lv6"]:
            if coin in eval(f"self.discount_data.{name}")["coin"]:
                ret = eval(f"self.discount_data.{name}")["info"]
                break
        if ret == []:
            ret = self.get_discount_apiInfo(coin)
        self.discount_info[coin]= ret
        return ret
    
    def get_discount_apiInfo(self, coin:str) -> list:
        """get discount information of special coin through api
        Args:
            coin (str): str.upper()
        Returns:
            list: list of dict
        """
        response = requests.get(self.discount_url + f"?ccy={coin.upper()}")
        if response.status_code == 200:
            ret = response.json()['data'][0]['discountInfo']
        else:
            ret = []
        self.discount_info[coin.upper()] = ret.copy()
        return ret
    
    def get_tier(self, instType, tdMode, instFamily=None, instId=None, tier=None, ccy = None) -> dict:
        params = {k:v  for k, v in locals().items() if k != 'self' and v is not None}
        url = self.parse_params_to_str(params)
        ret = requests.get(self.tiers_url+url)
        return ret.json()
    
    def parse_params_to_str(self, params: dict):
        url = '?'
        for key, value in params.items():
            url = url + str(key) + '=' + str(value) + '&'
        return url[0:-1]
    
    def handle_origin_tier(self, data: list) -> pd.DataFrame:
        """" data is the origin data return from okex api"""
        tiers = pd.DataFrame(columns = ["minSz", "maxSz", "mmr", "imr", "maxLever"])
        for i in range(len(data)):
            for col in tiers.columns:
                tiers.loc[i, col] = eval(data[i][col])
        return tiers
    
    def get_tier_uswap(self, coin: str) -> pd.DataFrame:
        if coin.upper() not in self.tier_uswap.keys():
            if os.path.isfile(f"{os.environ['HOME']}/data/tier/uswap/{coin}.csv"):
                ret = pd.read_csv(f"{os.environ['HOME']}/data/tier/uswap/{coin}.csv", index_col=0)
            else:
                ret = self.get_tier_swap(coin = coin.upper(), contract="USDT")
                os.mkdir(f"{os.environ['HOME']}/data/tier/uswap") if not os.path.exists(f"{os.environ['HOME']}/data/tier/uswap") else None
                ret.to_csv(f"{os.environ['HOME']}/data/tier/uswap/{coin}.csv")
            self.tier_uswap[coin] = ret
        else:
            ret = self.tier_uswap[coin.upper()]
        return ret
    
    def get_tier_cswap(self, coin: str) -> pd.DataFrame:
        if coin.upper() not in self.tier_cswap.keys():
            if os.path.isfile(f"{os.environ['HOME']}/data/tier/cswap/{coin}.csv"):
                ret = pd.read_csv(f"{os.environ['HOME']}/data/tier/cswap/{coin}.csv", index_col=0)
            else:
                ret = self.get_tier_swap(coin = coin.upper(), contract="USD")
                os.mkdir(f"{os.environ['HOME']}/data/tier/cswap") if not os.path.exists(f"{os.environ['HOME']}/data/tier/cswap") else None
                ret.to_csv(f"{os.environ['HOME']}/data/tier/cswap/{coin}.csv")
            self.tier_cswap[coin] = ret
        else:
            ret = self.tier_cswap[coin.upper()]
        return ret
    
    def get_tier_usdc(self, coin: str) -> pd.DataFrame:
        if coin.upper() not in self.tier_usdc.keys():
            if os.path.isfile(f"{os.environ['HOME']}/data/tier/usdc/{coin}.csv"):
                ret = pd.read_csv(f"{os.environ['HOME']}/data/tier/usdc/{coin}.csv", index_col=0)
            else:
                ret = self.get_tier_swap(coin = coin.upper(), contract="USDC")
                os.mkdir(f"{os.environ['HOME']}/data/tier/usdc") if not os.path.exists(f"{os.environ['HOME']}/data/tier/usdc") else None
                ret.to_csv(f"{os.environ['HOME']}/data/tier/usdc/{coin}.csv")
            self.tier_usdc[coin] = ret
        else:
            ret = self.tier_usdc[coin.upper()]
        return ret
    
    def get_tier_swap(self, coin: str, contract: str) -> pd.DataFrame:
        name = name = f"{coin.upper()}-{contract}"
        ret = self.get_tier(instType = "SWAP", 
                tdMode = "cross",
                instFamily= name,
                instId= name,
                tier="")
        data = ret["data"] if "data" in ret.keys() else []
        tier = self.handle_origin_tier(data)
        return tier
    
    def get_tier_spot(self, coin: str) -> pd.DataFrame:
        if coin.upper() not in self.tier_spot.keys():
            if os.path.isfile(f"{os.environ['HOME']}/data/tier/spot/{coin}.csv"):
                tier = pd.read_csv(f"{os.environ['HOME']}/data/tier/spot/{coin}.csv", index_col=0)
            else:
                ret = self.get_tier(instType = "MARGIN", 
                    tdMode = "cross",
                    ccy = coin.upper())
                tier = self.handle_origin_tier(ret["data"])
                os.mkdir(f"{os.environ['HOME']}/data/tier/spot") if not os.path.exists(f"{os.environ['HOME']}/data/tier/spot") else None
                tier.to_csv(f"{os.environ['HOME']}/data/tier/spot/{coin}.csv")
            self.tier_spot[coin] = tier
        else:
            tier = self.tier_spot[coin.upper()]
        return tier
    
    def find_mmr(self, amount: float, tier: pd.DataFrame) -> float:
        """
        Args:
            amount (float): the amount of spot asset or swap contract
            tier (pd.DataFrame): the position tier information
        """
        if amount <= 0:
            return 0
        else:
            mmr = np.nan
            for i in tier.index:
                if amount > tier.loc[i, "minSz"] and amount <= tier.loc[i, "maxSz"]:
                    mmr = tier.loc[i, "mmr"]
                    break
            return mmr
    
    def get_mmr_uswap(self, coin: str, amount: float) -> float:
        """get mmr of usdt_swap

        Args:
            coin (str): the name of coin, str.upper()
            amount (float): the coin number of usdt_swap asset, not dollar value, not contract number
        """
        coin = coin.upper()
        tier = self.tier_uswap[coin] if coin in self.tier_uswap.keys() else self.get_tier_uswap(coin)
        contractsize = self.get_contractsize_uswap(coin) if not coin in self.contractsize_uswap.keys() else self.contractsize_uswap[coin]
        num = amount / contractsize
        mmr = self.find_mmr(amount = num, tier = tier)
        return mmr
    
    def get_mmr_usdc(self, coin: str, amount: float) -> float:
        """get mmr of usdc_swap

        Args:
            coin (str): the name of coin, str.upper()
            amount (float): the coin number of usdt_swap asset, not dollar value, not contract number
        """
        coin = coin.upper()
        tier = self.tier_usdc[coin] if coin in self.tier_usdc.keys() else self.get_tier_usdc(coin)
        contractsize = self.get_contractsize_usdc(coin) if not coin in self.contractsize_usdc.keys() else self.contractsize_usdc[coin]
        num = amount / contractsize
        mmr = self.find_mmr(amount = num, tier = tier)
        return mmr
    
    def get_mmr_cswap(self, coin: str, amount: float) -> float:
        """get mmr of usd_swap

        Args:
            coin (str): the name of coin, str.upper()
            amount (float): the contract number of usd_swap asset, not dollar value, not coin number
        """
        coin = coin.upper()
        tier = self.tier_cswap[coin] if coin in self.tier_cswap.keys() else self.get_tier_cswap(coin)
        mmr = self.find_mmr(amount = amount, tier = tier)
        return mmr
    
    def get_mmr_spot(self, coin: str, amount: float) -> float:
        """get mmr of spot

        Args:
            coin (str): the name of coin, str.upper()
            amount (float): the number of spot asset, not dollar value
        """
        coin = coin.upper()
        tier = self.tier_spot[coin] if coin in self.tier_spot.keys() else self.get_tier_spot(coin)
        mmr = self.find_mmr(amount = amount, tier = tier)
        return mmr
    
    def get_mmr(self, coin: str, amount: float, contract: str) -> float:
        coin = coin.upper()
        contract = contract.replace("_", "-")
        if contract in ["usdt", "spot", "-usdt", "-spot"]:
            mmr = 0 if amount >=0 else self.get_mmr_spot(coin, -amount)
        elif contract in ["usdt-swap", "-usdt-swap"]:
            mmr = self.get_mmr_uswap(coin, abs(amount))
        elif contract in ["usd-swap", "-usd-swap"]:
            mmr = self.get_mmr_cswap(coin, abs(amount))
        elif contract in ["usdc-swap", "-usdc-swap"]:
            mmr = self.get_mmr_usdc(coin, abs(amount))
        else:
            mmr = np.nan
        return mmr
    
    def get_tiers(self, coin: str, contract: str) -> pd.DataFrame:
        coin = coin.upper()
        contract = contract.replace("_", "-")
        if contract in ["usdt-swap", "-usdt-swap"]:
            ret = self.get_tier_uswap(coin)
        elif contract in ["usd-swap", "-usd-swap"]:
            ret = self.get_tier_cswap(coin)
        elif contract in ["usdc-swap", "-usdc-swap"]:
            ret = self.get_tier_usdc(coin)
        else:
            ret = pd.DataFrame()
        return ret
    
    def get_contractsize(self, pair: str) -> float:
        pair = pair.replace("_", "-").lower()
        coin = pair.split("-")[0].upper()
        contract = pair.replace(coin.lower(), "")
        if contract == "-usdt-swap":
            size = self.get_contractsize_uswap(coin)
        elif contract == "-usd-swap":
            size = self.get_contractsize_cswap(coin)
        elif contract == "-usdc-swap":
            size = self.get_contractsize_usdc(coin)
        elif contract == "-usdt":
            size = 1
        else:
            size = np.nan
        return size