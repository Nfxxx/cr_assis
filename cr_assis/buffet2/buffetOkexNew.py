from cr_assis.connect.connectData import ConnectData
from cr_assis.account.accountOkex import AccountOkex
from cr_assis.account.initAccounts import InitAccounts
from github import Github
from github.Repository import Repository
import numpy as np
import pandas as pd
import os, datetime, logging, traceback, io, glob, json, copy, ccxt

class BuffetOkexNew(object):
    
    def __init__(self) -> None:
        self.folder = "pt"
        self.database = ConnectData()
        self.markets = ccxt.okex().load_markets()
        self.json_path = f"{os.environ['HOME']}/parameters/buffet2_config/pt"
        self.save_path = f"{os.environ['HOME']}/data/buffet2.0"
        self.accounts = {}
        self.is_long = {"long": 1, "short": 0}
        self.exchange_position = "okexv5"
        self.exchange_save = "okex"
        self.now_position = {}
        self.add = {}
        self.token_path = f"{os.environ['HOME']}/.git-credentials"
        self.parameter = {}
        self.coin_price = {}
        self.usd_contractsize = {}
        self.spreads = {}
        self.contractsize_path: str = os.environ['HOME'] + '/parameters/config_buffet/dt/contractsize.csv'
        self.parameter_cols = ['account', 'coin', 'portfolio', 'open', 'closemaker', 'position', 'closetaker', 'open2', 'closemaker2', 'position2', 'closetaker2', 'fragment',
                            'fragment_min', 'funding_stop_open', 'funding_stop_close', 'position_multiple', 'timestamp', 'is_long', 'chase_tick', 'master_pair', 'slave_pair', "master_secret", "slave_secret", "combo"]
        self.config_keys = set(["default_path", "total_mv", "single_mv", "thresh"])
        self.default_keys = set(['combo', 'funding_open', 'funding_close', 'chase_tick', 'close', 'open', 'closemaker', 'closetaker', 'closemaker2', 'closetaker2', 'cm2_change', 'fragment', 'fragment_min', 'open_add', 'close_add', 'select_u', 'select_ratio', 'maxloss', 'open_thresh', 'close_thresh', 'future_date'])
        self.execute_account: AccountOkex
        self.load_logger()
    
    def get_contractsize(self, symbol: str) -> float:
        ret = self.markets[symbol]["contractSize"] if symbol in self.markets.keys() else np.nan
        self.usd_contractsize[symbol.split("/")[0].upper()] = ret
        return ret
    
    def get_contractsize_cswap(self, coin: str) ->float:
        
        symbol = f"{coin}/USD:{coin}"
        contractsize = self.get_contractsize(symbol)
        self.usd_contractsize[coin] = contractsize
        return contractsize
    
    def get_usd_contractsize(self, coin: str) -> float:
        coin = coin.upper()
        ret = self.usd_contractsize[coin] if coin in self.usd_contractsize.keys() else self.get_contractsize(symbol = f"{coin}/USD:{coin}")
        return ret
    
    def get_redis_price(self, coin: str) -> float:
        ret = self.database.get_redis_data(key = f"{self.exchange_position}/{coin.lower()}-usdt")
        price = float(ret[b'ask0_price']) if b'ask0_price' in ret.keys() else np.nan
        self.coin_price[coin.upper()] = price
        return price
    
    def get_coin_price(self, coin: str) -> float:
        coin = coin.upper()
        ret = self.coin_price[coin] if coin in self.coin_price.keys() else self.get_redis_price(coin)
        return ret
    
    def check_config(self, file: str) -> dict:
        try:
            with open(file, "r") as f:
                data = json.load(f)
            ret = self.load_config_default(data) if set(data.keys()) == self.config_keys and len(data["total_mv"]) > 0 else {}
            if ret == {}:
                self.logger.warning(f"{file}因为key对不上或者total_mv填写错误所以没有加载默认config")
        except:
            ret = {}
            self.logger.warning(f"{file} 加载出错")
        return ret
    
    def connect_account_config(self, config: dict) -> dict:
        ret = {}
        for name in config["total_mv"].keys():
            ret[name] = config
        return ret
    
    def load_config_default(self, config: dict) -> None:
        try:
            path = os.environ["HOME"]+config["default_path"]
            self.logger.info(f"实际default路径为{path}")
            with open(path, "r") as f:
                data = json.load(f)
            config.update(data)
            ret = self.connect_account_config(config) if set(data.keys()) == self.default_keys else {}
            if ret == {}:
                self.logger.warning(f"{config}因为default文件的key对不上所以没有和账户绑定成功")
        except:
            ret = {}
            self.logger.warning(f"{config}的default加载错误")
        return ret
    
    def load_config(self) -> None:
        self.config = {}
        files = glob.glob(f"{self.json_path}/*.json")
        self.logger.info(f"获取到{self.json_path}路径下的所有json文件为{files}")
        for file in files:
            self.logger.info(f"开始读取{file}文件")
            self.config.update(self.check_config(file))
        self.logger.info(f"最终config指定需要出参的账户有{self.config.keys()}")
    
    def load_logger(self) -> None:
        Log_Format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        path_save = f"{self.save_path}/logs/{datetime.date.today()}/"
        os.makedirs(path_save) if not os.path.exists(path_save) else None
        name = datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")
        file_name = f"{path_save}{name}.log"
        logger = logging.getLogger(__name__)
        logger.setLevel(level=logging.DEBUG)
        handler = logging.FileHandler(filename=file_name, encoding="UTF-8")
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(Log_Format)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(Log_Format)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        self.logger = logger

    
    def check_account(self, account: AccountOkex) -> bool:
        account.get_account_position()
        account.get_mgnRatio()
        is_nan = False
        # 权益获取，获取不到跳过
        if not hasattr(account, "adjEq") or np.isnan(account.adjEq):
            self.logger.warning(f"{account.parameter_name}:获取equity错误")
            is_nan = True
        # 仓位获取，获取不到跳过
        if not hasattr(account, "position") or not hasattr(account, "now_position") or account.now_position["is_exposure"].sum() >0 or account.parameter_name in account.position["master_secret"].values or (hasattr(account, "position") and len(account.origin_position) == 0):
            self.logger.warning(f"{account.parameter_name}:最近10分钟position数据缺失或者有敞口")
            self.logger.info(account.position.to_dict())
            self.logger.info(account.now_position.to_dict())
            self.logger.info(account.origin_position.to_dict())
            is_nan = True
        # mr获取，获取不到跳过
        if not hasattr(account, "mr") or len(account.mr) == 0:
            self.logger.warning(f"{account.parameter_name}:没有获取到当前的mr")
            is_nan = True
        return is_nan
    
    def init_accounts(self):
        """初始化所有orch打开的账户 删除获取不到position、adjEq和mr的账户
        """
        self.load_config()
        init = InitAccounts(ignore_test=False)
        self.accounts = init.init_accounts_okex()
        names = set(self.accounts.keys())
        self.logger.info(f"orch打开的deploy_id有{init.deploy_ids}")
        self.logger.info(f"初始化orch打开的融合okex账户有{names}")
        for name in names:
            if name not in self.config.keys():
                self.accounts.pop(name, None)
                self.logger.info(f"{name}因为不在config指定出参中而被删除")
            elif self.check_account(account = self.accounts[name]):
                self.accounts.pop(name, None)
                self.logger.info(f"{name}因为check_account没有通过而被删除")
            else:
                self.logger.info(f"{name}config指定需要出参并且check_account通过")
        self.logger.info(f"最终确定需要出参的账户有{self.accounts.keys()}")
        return self.accounts
    
    def init_parameter(self) -> pd.DataFrame:
        """初始化parameter
        """
        self.logger.info(f"开始初始化{self.execute_account.parameter_name}的parameter")
        account = self.execute_account
        select_u = self.get_real_thresh(coin = "btc", combo = "ssf", thresh="select_u")
        select_ratio = self.get_real_thresh(coin = "btc", combo = "ssf", thresh="select_ratio")
        self.logger.info(f"select_u: {select_u}, select_ratio: {select_ratio}")
        now_pos = account.position[(account.position["MV"] > select_u) & (account.position["MV%"] > select_ratio)].copy().set_index("coin")
        self.logger.info(f"{self.execute_account.parameter_name}剔除忽略的mv后仓位为{now_pos.to_dict()}")
        self.now_position[account.parameter_name] = now_pos.copy()
        parameter = pd.DataFrame(columns=self.parameter_cols, index = now_pos.index)
        if len(now_pos) > 0:
            self.logger.info(f"{account.parameter_name}:非新账户, 初始化目前已经持有币的parameter参数")
            parameter['portfolio'] = 0
            cols = ["position", "master_pair", "slave_pair", "master_secret", "slave_secret", "combo"]
            parameter[cols] = now_pos[cols]
            parameter['is_long'] = now_pos['side'].apply(lambda x: 1 if x == 'long' else 0)
        else:
            self.logger.info(f"{account.parameter_name}:新账户, 初始化parameter, 参数默认为空")
        account.parameter = parameter
        self.logger.info(f"{self.execute_account.parameter_name} parameter初始化结束")
        
    def check_single_mv(self, coin: str, values: list) -> bool:
        is_error = True
        if len(values) != 2:
            self.logger.info(f"{self.execute_account.parameter_name}的single_mv填写错误, {coin}的value{values}长度不等于2, 无法进行单币种超限减仓和加仓")
        elif values[1] < 0:
            self.logger.info(f"{self.execute_account.parameter_name}的single_mv填写错误, {coin}的value{values}第二个数字小于0, 无法进行单币种超限减仓和加仓")
        elif abs(values[1]) < abs(values[0]):
            self.logger.info(f"{self.execute_account.parameter_name}的single_mv填写错误, {coin}的value{values}第二个数字绝对值小于第一个, 无法进行单币种超限减仓和加仓")
        else:
            is_error = False
        return is_error

    def execute_reduce(self, coin: str, to_mv: float) -> None:
        if coin not in self.execute_account.parameter.index or to_mv < 0:
            self.logger.info(f"execute_reduce失败, {coin} 不在{self.execute_account.parameter_name}持仓中或者{to_mv}小于0")
            return
        if to_mv > 0:
            self.execute_account.parameter.loc[coin, "position"] = self.now_position[self.execute_account.parameter_name].loc[coin, "position"] * to_mv / self.now_position[self.execute_account.parameter_name].loc[coin, "MV%"]
            self.execute_account.parameter.loc[coin, "portfolio"] = -2
            self.logger.info(f"execute_reduce成功, {self.execute_account.parameter_name}持仓中{coin}将二档减仓至{to_mv}")
        else:
            self.execute_account.parameter.loc[coin, "position"] = self.now_position[self.execute_account.parameter_name].loc[coin, "position"]
            self.execute_account.parameter.loc[coin, "portfolio"] = -1
            self.logger.info(f"execute_reduce成功, {self.execute_account.parameter_name}持仓中{coin}将一档减仓")
        self.now_position[self.execute_account.parameter_name].loc[coin, "position"] *= to_mv / self.now_position[self.execute_account.parameter_name].loc[coin, "MV%"]
        self.now_position[self.execute_account.parameter_name].loc[coin, "MV%"] = to_mv
    
    def execute_add(self, coin: str, to_mv: float, combo: str):
        parameter = self.execute_account.parameter
        if coin in self.now_position[self.execute_account.parameter_name].index and abs(to_mv) > self.now_position[self.execute_account.parameter_name].loc[coin, "MV%"]:
            parameter.loc[coin, "position"] *= max(abs(to_mv) / self.now_position[self.execute_account.parameter_name].loc[coin, "MV%"], 1)
            parameter.loc[coin, "portfolio"] = 1
            self.logger.info(f"{self.execute_account.parameter_name}已持有币种加仓{coin}@{combo}至{to_mv}")
        elif coin in self.now_position[self.execute_account.parameter_name].index and abs(to_mv) <= self.now_position[self.execute_account.parameter_name].loc[coin, "MV%"]:
            self.logger.info(f"{self.execute_account.parameter_name}已持有币种加仓{coin}@{combo}仓位mv{self.now_position[self.execute_account.parameter_name].loc[coin, 'MV%']}绝对值大于{to_mv}, 不加仓")
        else:
            price = self.get_usd_contractsize(coin) if combo.split("-")[0].split("_")[1] == "usd" else self.get_coin_price(coin)
            parameter.loc[coin, "position"] = abs(to_mv) / 100 * self.execute_account.adjEq / price
            parameter.loc[coin, "combo"] = combo
            parameter.loc[coin, "portfolio"] = 1
            parameter.loc[coin, "is_long"] = 1 if to_mv > 0 else 0
            parameter.loc[coin, ["master_pair", "slave_pair"]] = self.execute_account.get_pair_name(coin, combo)
            parameter.loc[coin, ["master_secret", "slave_secret"]] = self.execute_account.get_secret_name(coin, combo)
            self.logger.info(f"{self.execute_account.parameter_name}新币种加仓{coin}@{combo}至{to_mv}")
    
    def reduce_single_mv(self):
        self.logger.info(f"{self.execute_account.parameter_name}开始执行单币种mv减仓")
        now_position = self.now_position[self.execute_account.parameter_name]
        config = self.config[self.execute_account.parameter_name]
        for name, reduce in config["single_mv"].items():
            combo = config["combo"][name] if name in config["combo"].keys() else name
            for coin, values in reduce.items():
                if self.check_single_mv(coin, values):
                    continue
                if coin in now_position.index and now_position.loc[coin, "combo"] == combo and now_position.loc[coin, "MV%"] > values[1]:
                    self.logger.info(f"{self.execute_account.parameter_name}中{coin}@{combo}触发单币种超限减仓, 减仓至{values[1]}")
                    self.execute_reduce(coin, values[1])
        self.logger.info(f"{self.execute_account.parameter_name}单币种mv减仓结束")
        
    def reduce_total_mv(self):
        self.logger.info(f"{self.execute_account.parameter_name}开始执行总仓位mv减仓")
        now_position = self.now_position[self.execute_account.parameter_name]
        config = self.config[self.execute_account.parameter_name]["total_mv"]
        total_mv = now_position["MV%"].sum()
        plus = total_mv - config[self.execute_account.parameter_name][2]
        self.logger.info(f"{self.execute_account.parameter_name}现在总仓位为{total_mv}")
        self.logger.info(f"{self.execute_account.parameter_name}config中触发减仓的阈值为{config[self.execute_account.parameter_name][2]}")
        self.logger.info(f"{self.execute_account.parameter_name}超过仓位要求上限{plus}")
        if plus > 0:
            remove_mv = config[self.execute_account.parameter_name][1]
            self.logger.info(f"{self.execute_account.parameter_name}目前总仓位{total_mv}触发总仓位超限减仓, 将减仓至{remove_mv}")
            for coin in now_position.index:
                to_mv = remove_mv / total_mv
                self.execute_reduce(coin = coin, to_mv = to_mv)
        else:
            self.logger.info(f"{self.execute_account.parameter_name}目前总仓位{total_mv}没有触发总仓位超限减仓")
        self.logger.info(f"{self.execute_account.parameter_name}总仓位mv减仓结束")

    def get_add(self):
        real_add = {}
        now_position = self.now_position[self.execute_account.parameter_name]
        config = self.config[self.execute_account.parameter_name]
        for name, add in config["single_mv"].items():
            combo = config["combo"][name] if name in config["combo"].keys() else name
            for coin, values in add.items():
                if self.check_single_mv(coin, values) or coin in real_add.keys() or values[0] == 0:
                    continue
                if (coin.upper() == "ETH" and "beth" in now_position.index) or (coin.upper() == "BETH" and "eth" in now_position.index):
                    self.logger.info(f"因为{self.execute_account.parameter_name}仓位里有beth or eth所以不能加仓{coin}")
                    continue
                if coin not in self.execute_account.parameter.index and coin not in now_position.index:
                    real_add.update({f"{coin}@{combo}": values[0]})
                    self.logger.info(f"{coin}@{combo}是新币可以直接加仓")
                elif coin in now_position.index and now_position.loc[coin, "combo"] == combo and now_position.loc[coin, "MV%"] < abs(values[0]) and (now_position.loc[coin, "side"] == "long" and values[0] > 0 or now_position.loc[coin, "side"] == "short" and values[0] < 0):
                    real_add.update({f"{coin}@{combo}": values[0]})
                    self.logger.info(f"{coin}@{combo}已经在{self.execute_account.parameter_name}持仓中并且方向和combo相同可以加仓")
                else:
                    self.logger.info(f"{coin}@{combo}已经在{self.execute_account.parameter_name}持仓中但是方向和combo与持仓矛盾或者持仓超过可加仓上限不加仓")
        self.add[self.execute_account.parameter_name] = copy.deepcopy(real_add)
        self.logger.info(f"{self.execute_account.parameter_name}经过数据比较希望加仓的情况为{real_add}")
        return real_add
    
    def add_mv(self):
        self.logger.info(f"{self.execute_account.parameter_name}开始执行加仓")
        now_position = self.now_position[self.execute_account.parameter_name]
        config = self.config[self.execute_account.parameter_name]
        ava = config["total_mv"][self.execute_account.parameter_name][0] - now_position["MV%"].sum()
        self.logger.info(f"{self.execute_account.parameter_name}现在总仓位为{now_position['MV%'].sum()}")
        self.logger.info(f"{self.execute_account.parameter_name}config中加仓上限为{config['total_mv'][self.execute_account.parameter_name][0]}")
        self.logger.info(f"{self.execute_account.parameter_name}剩余可加仓位为{ava}")
        real_add = self.get_add()
        add_sum = pd.DataFrame.from_dict(real_add, orient='index').abs().values.sum()
        wish_add, holding_mv  = 0, 0
        hold = {}
        for k in real_add.keys():
            coin = k.split("@")[0]
            hold[coin] = now_position.loc[coin, "MV%"] if coin in now_position.index else 0
            wish_add += abs(real_add[k]) - hold[coin]
            holding_mv += hold[coin]
        add_to = holding_mv + min(wish_add, ava)
        for k, wish_mv in real_add.items():
            coin, combo = k.split("@")
            to_mv = - min(abs(wish_mv) / add_sum * add_to, abs(wish_mv), ava + hold[coin]) if wish_mv < 0 else min(abs(wish_mv) / add_sum * add_to, abs(wish_mv), ava + hold[coin])
            self.logger.info(f"{coin}@{combo}在{self.execute_account.parameter_name}持仓mv为{hold[coin]}, 账户剩余可加ava{ava}, 希望加仓wish_mv{wish_mv}, 总共想要加仓add_sum{add_sum}, 总共想要加仓到add_to{add_to}")
            self.logger.info(f"因此, {coin}@{combo}在{self.execute_account.parameter_name}中最终计算出来真正会加仓到min(abs(wish_mv) / add_sum * add_to{abs(wish_mv) / add_sum * add_to}, abs(wish_mv){abs(wish_mv)}, ava + hold[coin]{ava + hold[coin]})为{to_mv}")
            self.execute_add(coin, to_mv, combo)
        self.logger.info(f"{self.execute_account.parameter_name}加仓结束")
    
    def calc_up_thresh(self, spreads, threshold=50, up_down=0):
        self.logger.info(f"calc_up_thresh中spreads数据长度为{len(spreads)}, threshhold为{threshold}")
        spreads_avg = np.mean(spreads)
        spreads_minus_mean = spreads - spreads_avg
        up_amp = spreads_minus_mean.iloc[np.where(spreads_minus_mean > 0)]
        up_thresh = np.percentile(up_amp, [threshold]) + spreads_avg if len(up_amp) > 0 else [np.nan]
        up_thresh = up_thresh[0] + up_down
        return up_thresh
    
    def get_spreads_data(self, combo: str, coin: str, suffix: str = "") -> pd.DataFrame:
        coin = coin.lower()
        if combo in self.spreads.keys() and coin in self.spreads[combo].keys():
            ret = self.spreads[combo][coin]
        else:
            ret = self.execute_account.get_spreads(coin, combo, suffix)
            if combo not in self.spreads.keys():
                self.spreads[combo] = {coin: ret.copy()}
            else:
                self.spreads[combo][coin] = ret.copy()
        return ret
    
    def get_combo_abbreviation(self, combo: str) -> str:
        ret = combo
        for k, v in self.config[self.execute_account.parameter_name]["combo"].items():
            if v == combo:
                ret = k
                break
        return ret
    
    def get_real_thresh(self, coin: str, combo: str, thresh: str) -> str:
        name = self.execute_account.parameter_name
        config = self.config[name]["thresh"]
        abbreviation = self.get_combo_abbreviation(combo)
        if name in config.keys() and ((coin in config[name].keys() and thresh in config[name][coin].keys()) or ("all" in config[name].keys() and thresh in config[name]["all"].keys())) :
            ret = config[name][coin][thresh] if coin in config[name].keys() and thresh in config[name][coin].keys() else config[name]["all"][thresh]
        elif combo in config.keys() and ((coin in config[combo].keys() and thresh in config[combo][coin].keys()) or ("all" in config[combo].keys() and thresh in config[combo]["all"].keys())) :
            ret = config[combo][coin][thresh] if coin in config[combo].keys() and thresh in config[combo][coin].keys() else config[combo]["all"][thresh]
        elif abbreviation in config.keys() and ((coin in config[abbreviation].keys() and thresh in config[abbreviation][coin].keys()) or ("all" in config[abbreviation].keys() and thresh in config[abbreviation]["all"].keys())) :
            ret = config[abbreviation][coin][thresh] if coin in config[abbreviation].keys() and thresh in config[abbreviation][coin].keys() else config[abbreviation]["all"][thresh]
        elif "all" in config.keys() and ((coin in config["all"].keys() and thresh in config["all"][coin].keys()) or ("all" in config["all"].keys() and thresh in config["all"]["all"].keys())):
            ret = config["all"][coin][thresh] if coin in config["all"].keys() and thresh in config["all"][coin].keys() else config["all"]["all"][thresh]
        else:
            ret = self.config[name][thresh] if thresh in self.config[name].keys() else ""
        return ret
    
    def get_expect_thresh(self, coin: str, col: str, spread_name: str, use_thresh: str) -> float:
        ret = np.nan
        account = self.execute_account
        if coin in account.parameter.index:
            combo = self.execute_account.parameter.loc[coin, "combo"]
            specify_thresh = self.get_real_thresh(coin, combo, thresh = col)
            self.logger.info(f"{account.parameter_name}获取的{coin}@{combo}的{col}阈值为{specify_thresh}")
            if specify_thresh == "":
                self.logger.info(f"{account.parameter_name}没有指定{coin}@{combo}的{col}阈值")
                maxloss = float(self.get_real_thresh(coin, combo, thresh="maxloss"))
                self.logger.info(f"{account.parameter_name}中{coin}@{combo}的maxloss为{maxloss}")
                spread = self.get_spreads_data(combo, coin, suffix=self.get_real_thresh(coin, combo, thresh = "future_date"))
                ret = max(self.calc_up_thresh(spread[spread_name], threshold=float(self.get_real_thresh(coin, combo, thresh = use_thresh)), up_down=0) + float(self.get_real_thresh(coin, combo, thresh = f"{use_thresh.split('_')[0]}_add")), maxloss)
            else:
                self.logger.info(f"{account.parameter_name}指定了{coin}@{combo}的{col}阈值为{specify_thresh}")
                ret = float(specify_thresh)
        self.logger.info(f"{account.parameter_name}中{coin}@{combo}的{col}返回值最终确定为{ret}")
        return ret
    
    def get_open1(self, coin: str) -> float:
        spread_name = "bid0_spread" if self.execute_account.parameter.loc[coin, "is_long"] else "ask0_spread"
        return self.get_expect_thresh(coin, "open", spread_name, use_thresh="open_thresh")

    def get_closemaker(self, coin: str) -> float:
        spread_name = "ask0_spread" if self.execute_account.parameter.loc[coin, "is_long"] else "bid0_spread"
        return self.get_expect_thresh(coin, "closemaker", spread_name, use_thresh="close_thresh")
    
    def get_closemaker2(self, coin: str) -> float:
        spread_name = "ask0_spread" if self.execute_account.parameter.loc[coin, "is_long"] else "bid0_spread"
        combo = self.execute_account.parameter.loc[coin, "combo"] if coin in self.execute_account.parameter.index else ""
        ret = max(self.get_expect_thresh(coin, "closemaker2", spread_name, use_thresh="close_thresh") + float(self.get_real_thresh(coin, combo, thresh = "cm2_change")), float(self.get_real_thresh(coin, combo, thresh="maxloss"))) if self.get_real_thresh(coin, combo, thresh = "closemaker2") == "" else self.get_real_thresh(coin, combo, thresh = "closemaker2")
        return ret
    
    def get_open_close(self) -> pd.DataFrame:
        """处理开关仓阈值
        """
        parameter = self.execute_account.parameter
        for coin in parameter.index:
            level = parameter.loc[coin, "portfolio"]
            combo = parameter.loc[coin, "combo"]
            self.logger.info(f"{coin}@{combo}在{self.execute_account.parameter_name}中的level为{level}")
            parameter.loc[coin, "open"] = self.get_open1(coin) if level == 1 else 2
            parameter.loc[coin, "closemaker"] = self.get_closemaker(coin) if level == -1 else float(self.get_real_thresh(coin, combo, thresh="close"))
            parameter.loc[coin, "closemaker2"] = self.get_closemaker2(coin) if level == -2 or self.get_real_thresh(coin, combo, thresh="closemaker2") != "" else parameter.loc[coin, "closemaker"] + float(self.get_real_thresh(coin, combo, thresh="cm2_change"))
            parameter.loc[coin, "open2"] = parameter.loc[coin, "open"] + 1
            parameter.loc[coin, "closetaker"] = parameter.loc[coin, "closemaker"] + 0.001 if self.get_real_thresh(coin, combo, thresh="closetaker") == "" else float(self.get_real_thresh(coin, combo, thresh="closetaker"))
            parameter.loc[coin, "closetaker2"] = parameter.loc[coin, "closetaker"] + float(self.get_real_thresh(coin, combo, thresh="cm2_change")) if self.get_real_thresh(coin, combo, thresh="closetaker2") == "" else float(self.get_real_thresh(coin, combo, thresh="closetaker2"))
        self.arrange_parameter()
        return parameter
    
    def get_position2(self) -> pd.DataFrame:
        account = self.execute_account
        position = account.position.set_index("coin")
        for coin in account.parameter.index:
            hold = position.loc[coin, "position"] if coin in position.index else 0
            account.parameter.loc[coin, "position2"] = 2 * max(hold, account.parameter.loc[coin, "position"])
            self.logger.info(f"{account.parameter_name}中{coin}的持仓数量为{hold}, parameter中position为{account.parameter.loc[coin, 'position']}")
    
    def get_fragment(self) -> pd.DataFrame:
        account = self.execute_account
        for coin in account.parameter.index:
            combo = account.parameter.loc[coin, "combo"]
            coin_price = self.get_coin_price(coin = coin.upper()) if combo.split("_")[1] != "usd" else self.get_usd_contractsize(coin = coin.upper())
            account.parameter.loc[coin, ["fragment", "fragment_min"]] = [float(self.get_real_thresh(coin, combo, thresh="fragment")) / coin_price, float(self.get_real_thresh(coin, combo, thresh="fragment_min")) / coin_price]
    
    def handle_future_suffix(self) -> pd.DataFrame:
        account = self.execute_account
        for coin in account.parameter.index:
            combo = account.parameter.loc[coin, "combo"]
            account.parameter.loc[coin, "master_pair"] = account.parameter.loc[coin, "master_pair"].replace("future", self.get_real_thresh(coin, combo, thresh="future_date"))
            account.parameter.loc[coin, "slave_pair"] = account.parameter.loc[coin, "slave_pair"].replace("future", self.get_real_thresh(coin, combo, thresh="future_date"))
            account.parameter.loc[coin, "funding_stop_open"] =  float(self.get_real_thresh(coin, combo, thresh="funding_open"))
            account.parameter.loc[coin, "funding_stop_close"] = float(self.get_real_thresh(coin, combo, thresh="funding_close"))
            account.parameter.loc[coin, "chase_tick"] = float(self.get_real_thresh(coin, combo, thresh="chase_tick"))
        account.parameter["account"] = account.parameter_name
        account.parameter["position_multiple"] = 1
        account.parameter.set_index('account', inplace=True)
        account.parameter['coin'] = account.parameter["master_pair"]
        account.parameter['timestamp'] = datetime.datetime.utcnow() + datetime.timedelta(hours = 8, minutes= 3)
        account.parameter.dropna(how='all', axis=1, inplace=True)
        account.parameter.drop("combo", axis = 1, inplace=True) if "combo" in account.parameter.columns else None
    
    def arrange_parameter(self):
        self.get_position2()
        self.get_fragment()
        self.handle_future_suffix()
        
    def save_parameter(self): 
        path_save = f"{self.save_path}/parameter/{datetime.date.today()}/{self.exchange_save}"
        os.makedirs(path_save) if not os.path.exists(path_save) else None
        file_name = f'{path_save}/buffet2.0_parameter_{datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")}.xlsx'
        excel = pd.ExcelWriter(file_name)
        for name, parameter in self.parameter.items():
            parameter.to_excel(excel, sheet_name=name, index=True)
        excel.close()
    
    def check_total_mv(self) -> bool:
        is_error = True
        config = self.config[self.execute_account.parameter_name]["total_mv"][self.execute_account.parameter_name]
        if len(config) != 3:
            self.logger.warning(f"{self.execute_account.parameter_name}的total_mv填写错误, 长度不等于3")
        elif config[0] < 0 or config[1] < 0 or config[2] < 0:
            self.logger.warning(f"{self.execute_account.parameter_name}的total_mv填写错误, {config}中有小于0的数字")
        elif config[0] > config[1] or config[1] > config[2]:
            self.logger.warning(f"{self.execute_account.parameter_name}的total_mv填写错误, {config}中第一个数字大于第二个或者第二个数字大于第三个")
        else:
            is_error = False
        return is_error
    
    def get_parameter(self) -> None:
        """获得config里面写的并且orch打开的okex账户的parameter
        Args:
            is_save (bool, optional): save parameter. Defaults to True.
        """
        for name, account in self.accounts.items():
            self.logger.info(f"{name}开始出参")
            self.execute_account = account
            self.init_parameter()
            self.reduce_single_mv()
            if self.check_total_mv():
                self.logger.warning(f"{name}config中total_mv有误, 不进行总仓位减仓或加仓")
            else:
                self.add_mv() if self.now_position[account.parameter_name]["MV%"].sum() < self.config[account.parameter_name]["total_mv"][account.parameter_name][0] else self.reduce_total_mv()
            self.get_open_close()
            self.parameter[name] = account.parameter
            self.logger.info(f"{name}出参结束")
    
    def load_github(self) -> Repository:
        """加载github parameters仓库
        """
        with open(self.token_path, "r") as f:
            config = f.read()
        access_token = config.split(":")[-1].split("@")[0]
        g = Github(login_or_token=access_token)
        repo = g.get_repo("Coinrisings/parameters")
        self.repo = repo
        return repo
    
    def delete_parameter(self, folder: str) -> None:
        """删除原有文件
        """
        self.load_github() if not hasattr(self, "repo") else None
        repo = self.repo
        contents = repo.get_contents(f"excel/{folder}")
        if len(contents) >= 5:
            name = 'buffet2.0_parameter_' + str(datetime.datetime.utcnow() - pd.Timedelta('30m'))[:19].replace("-", "_").replace(
                " ", "_").replace(":", "_")
            for content_file in contents:
                if 'buffet2.0_parameter' in content_file.name and name > content_file.name:
                    repo.delete_file(content_file.path,
                                    message=f"buffet removes {content_file.name} at {datetime.datetime.utcnow()}",
                                    sha=content_file.sha)
                    self.logger.info(f"buffet removes {content_file.name} at {datetime.datetime.utcnow()}")
        else:
            pass
    
    def upload_parameter(self):
        repo = self.load_github()
        self.delete_parameter(folder = self.folder)
        towrite = io.BytesIO()
        writer = pd.ExcelWriter(towrite, engine='openpyxl')
        for sheet_name, parameter in self.parameter.items():
            parameter["timestamp"] = datetime.datetime.utcnow() + datetime.timedelta(hours = 8, minutes=3)
            parameter.to_excel(excel_writer=writer, sheet_name=sheet_name)
        writer.close()
        upload_time = datetime.datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")
        data = towrite.getvalue()
        name = f"excel/{self.folder}/buffet2.0_parameter_{upload_time}.xlsx"
        repo.create_file(name, f"uploaded by buffet at {upload_time}", data)  # 显示上传字段
        self.logger.info(f"{name} uploaded at {datetime.datetime.utcnow()}")
    
    def log_bug(self, e: Exception):
        self.logger.critical(e)
        self.logger.critical(traceback.format_exc())
        self.logger.handlers.clear()
    
    def run_buffet(self, is_save = True, upload = False) -> None:
        """main function
        Args:
            is_save (bool, optional): save excel or not. Defaults to True.
            upload (bool, optional): upload excel to github or not. Defaults to False.
        """
        self.init_accounts()
        self.get_parameter()
        if is_save and len(self.parameter):
            self.save_parameter()
        if upload:
            self.upload_parameter()
        self.logger.handlers.clear()