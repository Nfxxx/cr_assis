from cr_assis.pnl.ssfoPnl import SsfoPnl

class DtcPnl(SsfoPnl):
    
    def __init__(self, accounts: list) -> None:
        super().__init__(accounts)