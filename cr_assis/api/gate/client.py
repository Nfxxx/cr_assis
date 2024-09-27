import hmac, hashlib, yaml, os, yaml, time
import cr_assis.api.gate.consts as c
from cr_assis.api.okex.client import Client as ClientO

class Client(ClientO):
    
    def __init__(self) -> None:
        super().__init__()
        self.c = c
        self.api_url = self.c.API_URL
    
    def load_account_api(self) -> None:
        with open(f"{os.environ['HOME']}/.cr_assis/account_gate_api.yml", "rb") as f:
            data: list[dict] = yaml.load(f, Loader= yaml.SafeLoader)
        for info in data:
            if "name" in info.keys() and info["name"] == self.name:
                self.api_key = info["api_key"]
                self.secret_key = info["secret_key"]
    
    def gen_sign(self, method, url, query_string=None, payload_string=None):
        key = self.api_key       
        secret = self.secret_key     
        t = time.time()
        m = hashlib.sha512()
        m.update((payload_string or "").encode('utf-8'))
        hashed_payload = m.hexdigest()
        s = '%s\n%s\n%s\n%s\n%s' % (method, url, query_string or "", hashed_payload, t)
        sign = hmac.new(secret.encode('utf-8'), s.encode('utf-8'), hashlib.sha512).hexdigest()
        return {'KEY': key, 'Timestamp': str(t), 'SIGN': sign}
    
    def get_account_header(self, query: str, method: str = c.GET):
        self.header = {"accept": c.APPLICATION_JSON, "content-type": c.APPLICATION_JSON}
        self.header.update(self.gen_sign(method, query))