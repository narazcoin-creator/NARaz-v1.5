import os, sqlite3, time, json, secrets, hashlib, hmac, threading, asyncio
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from urllib.request import Request, urlopen

try:
    import websocket
except Exception:
    websocket = None
try:
    import websockets
except Exception:
    websockets = None

VERSION = "1.5.0"
HOST = os.getenv("NARAZ_HOST", "0.0.0.0")
PORT = int(os.getenv("NARAZ_PORT", "8080"))
WS_PORT = int(os.getenv("NARAZ_WS_PORT", str(PORT + 1)))
DB_PATH = os.getenv("NARAZ_DB", os.path.join(os.path.dirname(__file__), "naraz_v15.db"))

MAX_SUPPLY = 1_000_000_000
DEMO_GRANT = 10_000.0
FEE = 0.001
PAIRS = ("BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT")
BASES = tuple(p.split("/")[0] for p in PAIRS)

MARKET = {
    "items": {}, "books": {}, "trades": {}, "candles": {},
    "history": {}, "connected": False, "source": "Binance public market",
    "updated_at": 0, "stream_started_at": 0
}
MARKET_LOCK = threading.RLock()
WS_CLIENTS = set()
WS_LOOP = None

def db():
    c = sqlite3.connect(DB_PATH, timeout=20, check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=20000")
    return c

def now(): return time.time()

def password_hash(password, salt=None):
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 310_000)
    return salt.hex() + "$" + digest.hex()

def password_ok(password, stored):
    try:
        salt, digest = stored.split("$", 1)
        calc = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 310_000).hex()
        return hmac.compare_digest(calc, digest)
    except Exception:
        return False

def wallet_address(): return "NAR1" + secrets.token_hex(20)

def make_tx(sender, recipient, amount):
    return {"id": secrets.token_hex(16), "sender": sender, "recipient": recipient,
            "amount": float(amount), "timestamp": now()}

def block_hash(block):
    payload = {k: block[k] for k in ("index","timestamp","transactions","previous_hash","nonce")}
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()

def init_db():
    c = db()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS users(
      id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE COLLATE NOCASE NOT NULL,
      email TEXT UNIQUE COLLATE NOCASE NOT NULL, password_hash TEXT NOT NULL,
      wallet TEXT UNIQUE NOT NULL, created_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY,user_id INTEGER NOT NULL,created_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS blocks(idx INTEGER PRIMARY KEY,timestamp REAL NOT NULL,transactions TEXT NOT NULL,
      previous_hash TEXT NOT NULL,nonce INTEGER NOT NULL,hash TEXT UNIQUE NOT NULL);
    CREATE TABLE IF NOT EXISTS pending(id INTEGER PRIMARY KEY AUTOINCREMENT,tx_id TEXT UNIQUE NOT NULL,
      sender TEXT NOT NULL,recipient TEXT NOT NULL,amount REAL NOT NULL,timestamp REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS holdings(user_id INTEGER NOT NULL,asset TEXT NOT NULL,quantity REAL NOT NULL DEFAULT 0,
      avg_price REAL NOT NULL DEFAULT 0,PRIMARY KEY(user_id,asset));
    CREATE TABLE IF NOT EXISTS orders(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,symbol TEXT NOT NULL,
      side TEXT NOT NULL,type TEXT NOT NULL,price REAL,quantity REAL NOT NULL,remaining REAL NOT NULL,status TEXT NOT NULL,
      filled REAL NOT NULL DEFAULT 0,avg_fill REAL NOT NULL DEFAULT 0,fee REAL NOT NULL DEFAULT 0,
      created_at REAL NOT NULL,updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS trades(id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT NOT NULL,price REAL NOT NULL,
      quantity REAL NOT NULL,buy_order INTEGER,sell_order INTEGER,buyer INTEGER,seller INTEGER,fee REAL NOT NULL DEFAULT 0,
      timestamp REAL NOT NULL);
    """)
    if c.execute("SELECT COUNT(*) n FROM blocks").fetchone()["n"] == 0:
        genesis = {"index":0,"timestamp":now(),"transactions":[
            {"id":"GENESIS","sender":"SYSTEM","recipient":"NARAZ_NETWORK","amount":MAX_SUPPLY}],
            "previous_hash":"0","nonce":0}
        genesis["hash"] = block_hash(genesis)
        c.execute("INSERT INTO blocks VALUES(?,?,?,?,?,?)",
                  (0,genesis["timestamp"],json.dumps(genesis["transactions"]),"0",0,genesis["hash"]))
    c.commit(); c.close()

def chain_valid(c):
    rows = c.execute("SELECT * FROM blocks ORDER BY idx").fetchall()
    if not rows: return False
    previous = "0"
    for expected, row in enumerate(rows):
        if row["idx"] != expected or row["previous_hash"] != previous: return False
        block = {"index":row["idx"],"timestamp":row["timestamp"],
                 "transactions":json.loads(row["transactions"]),
                 "previous_hash":row["previous_hash"],"nonce":row["nonce"]}
        if block_hash(block) != row["hash"]: return False
        previous = row["hash"]
    return True

def nar_balance(c, wallet):
    total = 0.0
    for row in c.execute("SELECT transactions FROM blocks ORDER BY idx"):
        for tx in json.loads(row["transactions"]):
            if tx["sender"] == wallet: total -= float(tx["amount"])
            if tx["recipient"] == wallet: total += float(tx["amount"])
    for row in c.execute("SELECT sender,recipient,amount FROM pending"):
        if row["sender"] == wallet: total -= float(row["amount"])
        if row["recipient"] == wallet: total += float(row["amount"])
    return round(total, 8)

def mine_pending(c):
    rows = c.execute("SELECT * FROM pending ORDER BY id").fetchall()
    if not rows: return None
    index = c.execute("SELECT COALESCE(MAX(idx),-1)+1 n FROM blocks").fetchone()["n"]
    previous = c.execute("SELECT hash FROM blocks WHERE idx=?",(index-1,)).fetchone()["hash"]
    transactions = [{"id":r["tx_id"],"sender":r["sender"],"recipient":r["recipient"],
                     "amount":r["amount"],"timestamp":r["timestamp"]} for r in rows]
    block = {"index":index,"timestamp":now(),"transactions":transactions,"previous_hash":previous,"nonce":0}
    block["hash"] = block_hash(block)
    c.execute("INSERT INTO blocks VALUES(?,?,?,?,?,?)",
              (index,block["timestamp"],json.dumps(transactions,separators=(",",":")),previous,0,block["hash"]))
    c.execute("DELETE FROM pending"); c.commit()
    broadcast({"type":"chain","height":index,"hash":block["hash"]})
    return block

def seed_demo(c):
    if c.execute("SELECT id FROM users WHERE username='demo'").fetchone(): return
    wallet = wallet_address()
    uid = c.execute("INSERT INTO users(username,email,password_hash,wallet,created_at) VALUES(?,?,?,?,?)",
                    ("demo","demo@naraz.local",password_hash("demo12345"),wallet,now())).lastrowid
    for asset, quantity in (("USDT",10000),("BTC",0),("ETH",0),("BNB",0),("SOL",0)):
        c.execute("INSERT INTO holdings VALUES(?,?,?,?)",(uid,asset,quantity,1 if asset=="USDT" else 0))
    t = make_tx("TESTNET_FAUCET",wallet,DEMO_GRANT)
    c.execute("INSERT INTO pending(tx_id,sender,recipient,amount,timestamp) VALUES(?,?,?,?,?)",
              (t["id"],t["sender"],t["recipient"],t["amount"],t["timestamp"]))
    mine_pending(c); c.commit()

def auth(c, headers):
    value = headers.get("Authorization","")
    if not value.startswith("Bearer "): return None
    return c.execute("SELECT u.* FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token=?",
                      (value[7:].strip(),)).fetchone()

def market_rest():
    try:
        url = "https://api.binance.com/api/v3/ticker/24hr?symbols=[%22BTCUSDT%22,%22ETHUSDT%22,%22BNBUSDT%22,%22SOLUSDT%22]"
        req = Request(url,headers={"User-Agent":"NARaz/1.5"})
        data = json.loads(urlopen(req,timeout=7).read().decode())
        with MARKET_LOCK:
            for item in data:
                base=item["symbol"][:-4]
                MARKET["items"][base] = {"symbol":base,"price":float(item["lastPrice"]),
                    "change":float(item["priceChangePercent"]),"volume":float(item["volume"]),
                    "high":float(item["highPrice"]),"low":float(item["lowPrice"]),
                    "quote_volume":float(item["quoteVolume"]),"source":"Binance REST fallback","updated_at":now()}
            MARKET["connected"]=True; MARKET["updated_at"]=now()
        return True
    except Exception:
        return False

def market_book(base):
    base=base.upper()
    if base not in BASES: return {"symbol":base,"bids":[],"asks":[],"source":"invalid"}
    try:
        req=Request(f"https://api.binance.com/api/v3/depth?symbol={base}USDT&limit=20",
                    headers={"User-Agent":"NARaz/1.5"})
        data=json.loads(urlopen(req,timeout=6).read().decode())
        book={"symbol":base,"bids":[[float(p),float(q)] for p,q in data.get("bids",[])],
              "asks":[[float(p),float(q)] for p,q in data.get("asks",[])],
              "source":"Binance REST fallback","updated_at":now()}
        with MARKET_LOCK: MARKET["books"][base]=book
        return book
    except Exception:
        with MARKET_LOCK: return MARKET["books"].get(base,{"symbol":base,"bids":[],"asks":[]})

def normalize_book(base,data):
    return {"symbol":base,"bids":[[float(p),float(q)] for p,q in data.get("bids",[])],
            "asks":[[float(p),float(q)] for p,q in data.get("asks",[])],
            "source":"Binance WebSocket","updated_at":now()}

def broadcast(payload):
    if not websockets or not WS_CLIENTS or WS_LOOP is None: return
    async def send_all():
        msg=json.dumps(payload,separators=(",",":"))
        dead=[]
        for client in list(WS_CLIENTS):
            try: await client.send(msg)
            except Exception: dead.append(client)
        for client in dead: WS_CLIENTS.discard(client)
    try: asyncio.run_coroutine_threadsafe(send_all(),WS_LOOP)
    except Exception: pass

def market_stream():
    if websocket is None: return
    streams=[]
    for base in BASES:
        b=base.lower()
        streams += [f"{b}usdt@ticker",f"{b}usdt@depth20@100ms",f"{b}usdt@aggTrade",f"{b}usdt@kline_1m"]
    url="wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)
    while True:
        try:
            ws=websocket.create_connection(url,timeout=15)
            with MARKET_LOCK:
                MARKET["connected"]=True; MARKET["stream_started_at"]=now()
            broadcast({"type":"network","connected":True,"source":"Binance WebSocket"})
            while True:
                raw=ws.recv()
                if not raw: break
                outer=json.loads(raw); data=outer.get("data",{}); stream=outer.get("stream","")
                event=data.get("e","")
                if event=="24hrTicker":
                    symbol=data.get("s",""); base=symbol[:-4]
                    if base in BASES:
                        item={"symbol":base,"price":float(data["c"]),"change":float(data.get("P",0)),
                              "volume":float(data.get("v",0)),"high":float(data.get("h",0)),
                              "low":float(data.get("l",0)),"quote_volume":float(data.get("q",0)),
                              "source":"Binance WebSocket","updated_at":now()}
                        with MARKET_LOCK: MARKET["items"][base]=item; MARKET["updated_at"]=now()
                        history=MARKET["history"].setdefault(base,deque(maxlen=180))
                        history.append([item["updated_at"],item["price"]])
                        broadcast({"type":"ticker","item":item})
                elif event=="depthUpdate":
                    base=data.get("s","")[:-4]
                    if base in BASES:
                        book=normalize_book(base,{"bids":data.get("b",[]),"asks":data.get("a",[])})
                        with MARKET_LOCK: MARKET["books"][base]=book
                        broadcast({"type":"book","book":book})
                elif event=="aggTrade":
                    base=data.get("s","")[:-4]
                    if base in BASES:
                        trade={"id":str(data.get("a")),"symbol":base,"price":float(data["p"]),
                               "quantity":float(data["q"]),"buyer_maker":bool(data.get("m")),
                               "timestamp":data.get("T",int(now()*1000))}
                        trades=MARKET["trades"].setdefault(base,deque(maxlen=50)); trades.appendleft(trade)
                        broadcast({"type":"trade","trade":trade})
                elif event=="kline":
                    k=data.get("k",{}); base=data.get("s","")[:-4]
                    if base in BASES:
                        candle={"time":k.get("t"),"open":float(k["o"]),"high":float(k["h"]),
                                "low":float(k["l"]),"close":float(k["c"]),"volume":float(k["v"]),
                                "closed":bool(k.get("x"))}
                        candles=MARKET["candles"].setdefault(base,deque(maxlen=120))
                        if candles and candles[-1]["time"]==candle["time"]: candles[-1]=candle
                        else: candles.append(candle)
                        broadcast({"type":"candle","symbol":base,"candle":candle})
        except Exception:
            with MARKET_LOCK: MARKET["connected"]=False
            broadcast({"type":"network","connected":False,"source":"Binance WebSocket"})
            time.sleep(2)

def start_market():
    threading.Thread(target=market_stream,daemon=True,name="naraz-binance-stream").start()
    def fallback():
        while True:
            if not MARKET["items"]: market_rest()
            time.sleep(10)
    threading.Thread(target=fallback,daemon=True,name="naraz-market-fallback").start()

async def ws_handler(client):
    WS_CLIENTS.add(client)
    try:
        with MARKET_LOCK:
            snapshot={"type":"snapshot","network":"NARaz Network","version":VERSION,
                      "market":MARKET["items"],"books":MARKET["books"],
                      "trades":{k:list(v) for k,v in MARKET["trades"].items()},
                      "candles":{k:list(v) for k,v in MARKET["candles"].items()},
                      "connected":MARKET["connected"]}
        await client.send(json.dumps(snapshot,separators=(",",":")))
        await client.wait_closed()
    finally: WS_CLIENTS.discard(client)

def start_ws():
    global WS_LOOP
    if websockets is None: return
    def runner():
        global WS_LOOP
        loop=asyncio.new_event_loop(); asyncio.set_event_loop(loop); WS_LOOP=loop
        async def boot():
            await websockets.serve(ws_handler,HOST,WS_PORT)
            await asyncio.Future()
        loop.run_until_complete(boot()); loop.run_forever()
    threading.Thread(target=runner,daemon=True,name="naraz-network-ws").start()

def body(handler):
    length=int(handler.headers.get("Content-Length","0"))
    return json.loads(handler.rfile.read(length) or b"{}")

class Handler(BaseHTTPRequestHandler):
    def log_message(self,*_): pass
    def send_json(self,payload,status=200):
        raw=json.dumps(payload,ensure_ascii=False).encode()
        self.send_response(status); self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Content-Length",str(len(raw))); self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Access-Control-Allow-Headers","Content-Type, Authorization")
        self.send_header("Access-Control-Allow-Methods","GET, POST, OPTIONS"); self.end_headers(); self.wfile.write(raw)
    def do_OPTIONS(self): self.send_json({"ok":True})
    def do_GET(self):
        c=db(); path=urlparse(self.path).path
        try:
            if path=="/api/health": return self.send_json({"ok":True,"service":"NARaz Network","version":VERSION})
            if path=="/api/network":
                return self.send_json({"ok":True,"network":"NARaz Network","version":VERSION,
                    "chain":{"height":c.execute("SELECT COALESCE(MAX(idx),0) n FROM blocks").fetchone()["n"],"valid":chain_valid(c)},
                    "market":{"connected":MARKET["connected"],"source":MARKET["source"],
                              "updated_at":MARKET["updated_at"]},"http_port":PORT,"websocket_port":WS_PORT,"pairs":PAIRS})
            if path=="/api/market":
                if not MARKET["items"]: market_rest()
                with MARKET_LOCK: return self.send_json({"ok":True,"items":list(MARKET["items"].values()),
                    "connected":MARKET["connected"],"source":MARKET["source"]})
            if path=="/api/market/overview":
                if not MARKET["items"]: market_rest()
                with MARKET_LOCK:
                    items=list(MARKET["items"].values())
                    gainers=sorted(items,key=lambda x:x.get("change",0),reverse=True)
                    losers=list(reversed(gainers))
                    return self.send_json({"ok":True,"items":items,"gainers":gainers[:3],"losers":losers[:3],
                        "updated_at":MARKET["updated_at"],"stream_connected":MARKET["connected"]})
            if path.startswith("/api/market/history/"):
                base=path.rsplit("/",1)[-1].upper()
                with MARKET_LOCK: data=list(MARKET["history"].get(base,[]))
                return self.send_json({"ok":True,"symbol":base,"history":data})
            if path.startswith("/api/orderbook/"):
                base=path.rsplit("/",1)[-1].upper()
                return self.send_json({"ok":True,"book":market_book(base)})
            if path.startswith("/api/trades/"):
                base=path.rsplit("/",1)[-1].upper()
                with MARKET_LOCK: return self.send_json({"ok":True,"trades":list(MARKET["trades"].get(base,[]))})
            if path=="/api/chain":
                rows=c.execute("SELECT * FROM blocks ORDER BY idx DESC LIMIT 50").fetchall()
                return self.send_json({"ok":True,"valid":chain_valid(c),
                    "height":c.execute("SELECT COALESCE(MAX(idx),0) n FROM blocks").fetchone()["n"],
                    "blocks":[{"index":r["idx"],"timestamp":r["timestamp"],"transactions":json.loads(r["transactions"]),
                               "previous_hash":r["previous_hash"],"hash":r["hash"]} for r in rows]})
            if path=="/api/me":
                u=auth(c,self.headers)
                if not u: return self.send_json({"ok":False,"error":"AUTH_REQUIRED"},401)
                return self.send_json({"ok":True,"user":{"id":u["id"],"username":u["username"],"email":u["email"],
                    "wallet":u["wallet"],"naraz":nar_balance(c,u["wallet"])}})
            if path=="/api/wallet":
                u=auth(c,self.headers)
                if not u: return self.send_json({"ok":False,"error":"AUTH_REQUIRED"},401)
                rows=c.execute("SELECT asset,quantity,avg_price FROM holdings WHERE user_id=?",(u["id"],)).fetchall()
                return self.send_json({"ok":True,"address":u["wallet"],"naraz":nar_balance(c,u["wallet"]),
                                       "holdings":[dict(r) for r in rows]})
            if path=="/api/orders":
                u=auth(c,self.headers)
                if not u: return self.send_json({"ok":False,"error":"AUTH_REQUIRED"},401)
                rows=c.execute("SELECT * FROM orders WHERE user_id=? ORDER BY id DESC LIMIT 100",(u["id"],)).fetchall()
                return self.send_json({"ok":True,"orders":[dict(r) for r in rows]})
            return self.send_json({"ok":False,"error":"NOT_FOUND"},404)
        finally: c.close()
    def do_POST(self):
        c=db(); path=urlparse(self.path).path
        try:
            data=body(self)
            if path=="/api/register":
                username=str(data.get("username","")).strip(); email=str(data.get("email","")).strip().lower(); password=str(data.get("password",""))
                if not (3<=len(username)<=32) or len(password)<8 or "@" not in email:
                    return self.send_json({"ok":False,"error":"INVALID_REGISTRATION"},400)
                if c.execute("SELECT 1 FROM users WHERE username=? COLLATE NOCASE OR email=? COLLATE NOCASE",(username,email)).fetchone():
                    return self.send_json({"ok":False,"error":"ACCOUNT_EXISTS"},409)
                wallet=wallet_address()
                uid=c.execute("INSERT INTO users(username,email,password_hash,wallet,created_at) VALUES(?,?,?,?,?)",
                              (username,email,password_hash(password),wallet,now())).lastrowid
                c.execute("INSERT INTO holdings VALUES(?,?,?,?)",(uid,"USDT",10000.0,1.0))
                grant=make_tx("TESTNET_FAUCET",wallet,DEMO_GRANT)
                c.execute("INSERT INTO pending(tx_id,sender,recipient,amount,timestamp) VALUES(?,?,?,?,?)",
                          (grant["id"],grant["sender"],grant["recipient"],grant["amount"],grant["timestamp"]))
                mine_pending(c)
                token=secrets.token_urlsafe(32); c.execute("INSERT INTO sessions VALUES(?,?,?)",(token,uid,now())); c.commit()
                return self.send_json({"ok":True,"token":token,"user":{"id":uid,"username":username,"email":email,"wallet":wallet,"naraz":DEMO_GRANT}})
            if path=="/api/login":
                identifier=str(data.get("identifier","")).strip(); password=str(data.get("password",""))
                user=c.execute("SELECT * FROM users WHERE username=? COLLATE NOCASE OR email=? COLLATE NOCASE",(identifier,identifier)).fetchone()
                if not user or not password_ok(password,user["password_hash"]): return self.send_json({"ok":False,"error":"INVALID_CREDENTIALS"},401)
                token=secrets.token_urlsafe(32); c.execute("INSERT INTO sessions VALUES(?,?,?)",(token,user["id"],now())); c.commit()
                return self.send_json({"ok":True,"token":token,"user":{"id":user["id"],"username":user["username"],"email":user["email"],"wallet":user["wallet"],"naraz":nar_balance(c,user["wallet"])}})
            if path=="/api/logout":
                ah=self.headers.get("Authorization",""); token=ah[7:].strip() if ah.startswith("Bearer ") else ""
                c.execute("DELETE FROM sessions WHERE token=?",(token,)); c.commit(); return self.send_json({"ok":True})
            if path=="/api/faucet":
                u=auth(c,self.headers)
                if not u: return self.send_json({"ok":False,"error":"AUTH_REQUIRED"},401)
                amount=min(float(data.get("amount",1000)),10000)
                grant=make_tx("TESTNET_FAUCET",u["wallet"],amount)
                c.execute("INSERT INTO pending(tx_id,sender,recipient,amount,timestamp) VALUES(?,?,?,?,?)",(grant["id"],grant["sender"],grant["recipient"],grant["amount"],grant["timestamp"]))
                block=mine_pending(c); return self.send_json({"ok":True,"transaction":grant,"block":block})
            if path=="/api/transfer":
                u=auth(c,self.headers)
                if not u: return self.send_json({"ok":False,"error":"AUTH_REQUIRED"},401)
                recipient=str(data.get("recipient","")).strip(); amount=float(data.get("amount",0))
                if not recipient.startswith("NAR1") or len(recipient)!=44 or amount<=0: return self.send_json({"ok":False,"error":"INVALID_TRANSFER"},400)
                if recipient==u["wallet"]: return self.send_json({"ok":False,"error":"SELF_TRANSFER"},400)
                if nar_balance(c,u["wallet"])+1e-12<amount: return self.send_json({"ok":False,"error":"INSUFFICIENT_NARAZ_BALANCE"},400)
                tx=make_tx(u["wallet"],recipient,amount)
                c.execute("INSERT INTO pending(tx_id,sender,recipient,amount,timestamp) VALUES(?,?,?,?,?)",(tx["id"],tx["sender"],tx["recipient"],tx["amount"],tx["timestamp"]))
                block=mine_pending(c); return self.send_json({"ok":True,"transaction":tx,"block":block})
            if path=="/api/order": return self.place_order(c,data)
            if path=="/api/cancel":
                u=auth(c,self.headers)
                if not u: return self.send_json({"ok":False,"error":"AUTH_REQUIRED"},401)
                oid=int(data.get("id",0)); c.execute("UPDATE orders SET status='CANCELLED',remaining=0,updated_at=? WHERE id=? AND user_id=? AND status='OPEN'",(now(),oid,u["id"])); c.commit()
                return self.send_json({"ok":True})
            return self.send_json({"ok":False,"error":"NOT_FOUND"},404)
        except Exception as exc:
            c.rollback(); return self.send_json({"ok":False,"error":"SERVER_ERROR","detail":str(exc)},500)
        finally: c.close()
    def place_order(self,c,data):
        u=auth(c,self.headers)
        if not u: return self.send_json({"ok":False,"error":"AUTH_REQUIRED"},401)
        symbol=str(data.get("symbol","")).upper(); side=str(data.get("side","")).upper(); typ=str(data.get("type","MARKET")).upper()
        qty=float(data.get("quantity",0)); price=float(data.get("price",0) or 0)
        if symbol not in PAIRS or side not in ("BUY","SELL") or typ not in ("MARKET","LIMIT") or qty<=0: return self.send_json({"ok":False,"error":"INVALID_ORDER"},400)
        base=symbol.split("/")[0]; market_price=(MARKET["items"].get(base) or {}).get("price")
        if not market_price: market_rest(); market_price=(MARKET["items"].get(base) or {}).get("price")
        if not market_price: return self.send_json({"ok":False,"error":"MARKET_UNAVAILABLE"},503)
        if typ=="MARKET":
            exec_price=market_price
            if side=="BUY":
                needed=qty*exec_price*(1+FEE); row=c.execute("SELECT quantity FROM holdings WHERE user_id=? AND asset='USDT'",(u["id"],)).fetchone()
                available=float(row["quantity"]) if row else 0
                if available+1e-12<needed: return self.send_json({"ok":False,"error":"INSUFFICIENT_BALANCE"},400)
                c.execute("UPDATE holdings SET quantity=quantity-? WHERE user_id=? AND asset='USDT'",(needed,u["id"]))
                row=c.execute("SELECT quantity FROM holdings WHERE user_id=? AND asset=?",(u["id"],base)).fetchone(); old=float(row["quantity"]) if row else 0
                c.execute("INSERT OR REPLACE INTO holdings VALUES(?,?,?,?)",(u["id"],base,old+qty,exec_price)); fee=qty*exec_price*FEE
            else:
                row=c.execute("SELECT quantity FROM holdings WHERE user_id=? AND asset=?",(u["id"],base)).fetchone(); available=float(row["quantity"]) if row else 0
                if available+1e-12<qty: return self.send_json({"ok":False,"error":"INSUFFICIENT_BALANCE"},400)
                c.execute("UPDATE holdings SET quantity=quantity-? WHERE user_id=? AND asset=?",(qty,u["id"],base))
                value=qty*exec_price*(1-FEE); row=c.execute("SELECT quantity FROM holdings WHERE user_id=? AND asset='USDT'",(u["id"],)).fetchone(); old=float(row["quantity"]) if row else 0
                c.execute("INSERT OR REPLACE INTO holdings VALUES(?,?,?,?)",(u["id"],"USDT",old+value,1)); fee=qty*exec_price*FEE
            oid=c.execute("INSERT INTO orders(user_id,symbol,side,type,price,quantity,remaining,status,filled,avg_fill,fee,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           (u["id"],symbol,side,typ,exec_price,qty,0,"FILLED",qty,exec_price,fee,now(),now())).lastrowid
            c.execute("INSERT INTO trades(symbol,price,quantity,buy_order,sell_order,buyer,seller,fee,timestamp) VALUES(?,?,?,?,?,?,?,?,?)",
                      (symbol,exec_price,qty,oid if side=="BUY" else None,oid if side=="SELL" else None,u["id"] if side=="BUY" else None,u["id"] if side=="SELL" else None,fee,now()))
            c.commit(); return self.send_json({"ok":True,"order_id":oid,"status":"FILLED","price":exec_price,"filled":qty,"fee":fee,"execution":"NARaz testnet"})
        if price<=0: return self.send_json({"ok":False,"error":"INVALID_PRICE"},400)
        oid=c.execute("INSERT INTO orders(user_id,symbol,side,type,price,quantity,remaining,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                      (u["id"],symbol,side,typ,price,qty,qty,"OPEN",now(),now())).lastrowid
        c.commit(); self.match(c,symbol); return self.send_json({"ok":True,"order_id":oid,"status":"OPEN","execution":"NARaz testnet"})
    def match(self,c,symbol):
        while True:
            buy=c.execute("SELECT * FROM orders WHERE symbol=? AND side='BUY' AND status='OPEN' ORDER BY price DESC,id ASC LIMIT 1",(symbol,)).fetchone()
            sell=c.execute("SELECT * FROM orders WHERE symbol=? AND side='SELL' AND status='OPEN' ORDER BY price ASC,id ASC LIMIT 1",(symbol,)).fetchone()
            if not buy or not sell or buy["price"]<sell["price"]: break
            qty=min(buy["remaining"],sell["remaining"]); price=sell["price"]; base=symbol.split("/")[0]; value=qty*price
            cash=c.execute("SELECT quantity FROM holdings WHERE user_id=? AND asset='USDT'",(buy["user_id"],)).fetchone()
            coin=c.execute("SELECT quantity FROM holdings WHERE user_id=? AND asset=?",(sell["user_id"],base)).fetchone()
            if not cash or float(cash["quantity"])<value*(1+FEE):
                c.execute("UPDATE orders SET status='CANCELLED',remaining=0,updated_at=? WHERE id=?",(now(),buy["id"])); c.commit(); continue
            if not coin or float(coin["quantity"])<qty:
                c.execute("UPDATE orders SET status='CANCELLED',remaining=0,updated_at=? WHERE id=?",(now(),sell["id"])); c.commit(); continue
            c.execute("UPDATE holdings SET quantity=quantity-? WHERE user_id=? AND asset='USDT'",(value*(1+FEE),buy["user_id"]))
            c.execute("UPDATE holdings SET quantity=quantity+? WHERE user_id=? AND asset=?",(qty,buy["user_id"],base))
            c.execute("UPDATE holdings SET quantity=quantity-? WHERE user_id=? AND asset=?",(qty,sell["user_id"],base))
            c.execute("UPDATE holdings SET quantity=quantity+? WHERE user_id=? AND asset='USDT'",(value*(1-FEE),sell["user_id"]))
            br=buy["remaining"]-qty; sr=sell["remaining"]-qty
            c.execute("UPDATE orders SET remaining=?,filled=quantity-?,avg_fill=?,status=?,updated_at=? WHERE id=?",(max(0,br),max(0,br),price,"FILLED" if br<=1e-12 else "OPEN",now(),buy["id"]))
            c.execute("UPDATE orders SET remaining=?,filled=quantity-?,avg_fill=?,status=?,updated_at=? WHERE id=?",(max(0,sr),max(0,sr),price,"FILLED" if sr<=1e-12 else "OPEN",now(),sell["id"]))
            c.execute("INSERT INTO trades(symbol,price,quantity,buy_order,sell_order,buyer,seller,fee,timestamp) VALUES(?,?,?,?,?,?,?,?,?)",(symbol,price,qty,buy["id"],sell["id"],buy["user_id"],sell["user_id"],value*FEE,now()))
            c.commit()

def main():
    init_db(); c=db(); seed_demo(c); c.close()
    market_rest(); start_market(); start_ws()
    print(f"NARaz Network v{VERSION}: HTTP {PORT}, WS {WS_PORT}")
    ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()

if __name__=="__main__": main()
