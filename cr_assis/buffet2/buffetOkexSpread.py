from cr_assis.buffet2.buffetOkexNew import BuffetOkexNew
import copy, datetime
import pandas as pd
import numpy as np

class BuffetOkexSpread(BuffetOkexNew):
    
    def __init__(self) -> None:
        super().__init__()
        self.default_keys = set(['combo', 'funding_open', 'funding_close', 'chase_tick', 'close', 'open', 'closemaker', 'closetaker', 'closemaker2', 'closetaker2', 'cm2_change', 'fragment', 'fragment_min', 'open_add', 'close_add', 'select_u', 'select_ratio', 'maxloss', 'open_thresh', 'close_thresh', 'future_date', 'spread_start'])
        
    def get_add(self) -> dict[str, dict[str, float]]:
        real_add = {}
        now_position = self.now_position[self.execute_account.parameter_name]
        config = self.config[self.execute_account.parameter_name]
        for name, add in config["single_mv"].items():
            combo = config["combo"][name] if name in config["combo"].keys() else name
            for coin, values in add.items():
                if self.check_single_mv(coin, values) or coin in real_add.keys() or values[0] == 0:
                    continue
                if coin not in self.execute_account.parameter.index and coin not in now_position.index:
                    real_add.update({f"{coin}@{combo}": values[0]})
                elif coin in now_position.index and now_position.loc[coin, "combo"] == combo and now_position.loc[coin, "MV%"] < abs(values[0]):
                    real_add.update({f"{coin}@{combo}": values[0]})
        self.add[self.execute_account.parameter_name] = copy.deepcopy(real_add)
        return real_add
    
    def duplicate_parameter(self) -> pd.DataFrame:
        df = self.execute_account.parameter.copy()
        df["is_long"] = df["is_long"].apply(lambda x: 0 if x else 1)
        self.execute_account.parameter = pd.concat([self.execute_account.parameter, df])
        self.execute_account.parameter.index = range(len(self.execute_account.parameter))
    
    def get_expect_thresh(self, coin: str, col: str, spread_name: str, use_thresh: str, combo: str) -> float:
        ret = np.nan
        specify_thresh = self.get_real_thresh(coin, combo, thresh = col)
        if specify_thresh == "":
            maxloss = float(self.get_real_thresh(coin, combo, thresh="maxloss"))
            spread = self.get_spreads_data(combo, coin, suffix=self.get_real_thresh(coin, combo, thresh = "future_date"))
            ret = max(self.calc_up_thresh(spread[spread_name], threshold=float(self.get_real_thresh(coin, combo, thresh = use_thresh)), up_down=0) + float(self.get_real_thresh(coin, combo, thresh = f"{use_thresh.split('_')[0]}_add")), maxloss)
        else:
            ret = float(specify_thresh)
        return ret
    
    def get_spreads_data(self, combo: str, coin: str, suffix: str = "") -> pd.DataFrame:
        coin = coin.lower()
        spread_start = self.get_real_thresh(coin, combo, thresh="spread_start")
        if combo in self.spreads.keys() and coin in self.spreads[combo].keys():
            ret = self.spreads[combo][coin]
        else:
            ret = self.execute_account.get_spreads(coin, combo, suffix,start=spread_start)
            if combo not in self.spreads.keys():
                self.spreads[combo] = {coin: ret.copy()}
            else:
                self.spreads[combo][coin] = ret.copy()
        return ret
    
    def get_open1(self, coin: str, combo: str, is_long: bool) -> float:
        spread_name = "bid0_spread" if is_long else "ask0_spread"
        return self.get_expect_thresh(coin, "open", spread_name, use_thresh="open_thresh", combo = combo)
    
    def get_closemaker(self, coin: str, combo: str, is_long: bool) -> float:
        spread_name = "ask0_spread" if is_long  else "bid0_spread"
        return self.get_expect_thresh(coin, "closemaker", spread_name, use_thresh="close_thresh", combo = combo)
    
    def get_open_close(self) -> pd.DataFrame:
        """处理开关仓阈值
        """
        self.arrange_parameter()
        self.duplicate_parameter()
        parameter = self.execute_account.parameter
        for i in parameter.index:
            coin = parameter.loc[i, "coin"].split("-")[0]
            combo = parameter.loc[i, "combo"]
            is_long = parameter.loc[i, "is_long"]
            parameter.loc[i, "open"] = self.get_open1(coin, combo, is_long) if self.get_real_thresh(coin, combo, thresh="open") == "" else float(self.get_real_thresh(coin, combo, thresh="open"))
            parameter.loc[i, "closemaker"] = self.get_closemaker(coin, combo, is_long) if self.get_real_thresh(coin, combo, thresh="closemaker") == "" else float(self.get_real_thresh(coin, combo, thresh="closemaker"))
            parameter.loc[i, "closemaker2"] = parameter.loc[i, "closemaker"] + float(self.get_real_thresh(coin, combo, thresh="cm2_change")) if self.get_real_thresh(coin, combo, thresh="closemaker2") != "" else parameter.loc[i, "closemaker"] + float(self.get_real_thresh(coin, combo, thresh="cm2_change"))
            parameter.loc[i, "open2"] = parameter.loc[i, "open"] + 1
            parameter.loc[i, "closetaker"] = parameter.loc[i, "closemaker"] + 0.001 if self.get_real_thresh(coin, combo, thresh="closetaker") == "" else float(self.get_real_thresh(coin, combo, thresh="closetaker"))
            parameter.loc[i, "closetaker2"] = parameter.loc[i, "closetaker"]+ float(self.get_real_thresh(coin, combo, thresh="cm2_change")) if self.get_real_thresh(coin, combo, thresh="closetaker2") == "" else float(self.get_real_thresh(coin, combo, thresh="closetaker2"))
        parameter.drop("combo", axis = 1, inplace=True) if "combo" in parameter.columns else None
        parameter.set_index("account", inplace=True)
        parameter['timestamp'] = datetime.datetime.utcnow() + datetime.timedelta(hours = 8, minutes= 3)
        return parameter
    
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
        account.parameter['coin'] = account.parameter["master_pair"]
        
        