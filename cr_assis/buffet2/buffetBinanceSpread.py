from buffetOkexSpread import BuffetOkexSpread
from cr_assis.account.accountBinance import AccountBinance
from cr_assis.account.initAccounts import InitAccounts
import ccxt

class BuffetBinanceSpread(BuffetOkexSpread):
    
    def __init__(self) -> None:
        super().__init__()
        self.markets = ccxt.binance().load_markets()
        self.exchange_position = "binance"
        self.exchange_save = "binance"
        self.execute_account: AccountBinance
    
    def init_accounts(self):
        """初始化所有orch打开的账户 删除获取不到position、adjEq和mr的账户
        """
        self.load_config()
        init = InitAccounts(ignore_test=False)
        self.accounts = init.init_accounts_binance()
        names = set(self.accounts.keys())
        for name in names:
            self.accounts.pop(name, None) if name not in self.config.keys() or self.check_account(account = self.accounts[name]) else None
        return self.accounts