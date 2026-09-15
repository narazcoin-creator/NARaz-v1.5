import json, socket, threading, time, secrets

DISCOVERY_PORT = 39555
MAGIC = "NARAZ_NETWORK_V1"

class NetworkDiscovery:
    def __init__(self, http_port, ws_port, host="0.0.0.0"):
        self.http_port=http_port
        self.ws_port=ws_port
        self.host=host
        self.node_id="NARNODE-"+secrets.token_hex(6).upper()
        self.started_at=time.time()
        self.peers={}
        self._stop=threading.Event()

    def payload(self):
        return {"magic":MAGIC,"node_id":self.node_id,"http_port":self.http_port,
                "ws_port":self.ws_port,"service":"NARaz Network","version":"1.5.1",
                "timestamp":time.time()}

    def _broadcast_loop(self):
        s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
        s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        msg=json.dumps(self.payload(),separators=(",",":" )).encode()
        while not self._stop.is_set():
            try: s.sendto(msg,("255.255.255.255",DISCOVERY_PORT))
            except OSError: pass
            self._stop.wait(3)
        s.close()

    def _listen_loop(self):
        s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        try: s.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
        except OSError: pass
        s.bind((self.host,DISCOVERY_PORT))
        s.settimeout(1)
        while not self._stop.is_set():
            try: raw,addr=s.recvfrom(8192)
            except socket.timeout: continue
            except OSError: break
            try: data=json.loads(raw.decode())
            except Exception: continue
            if data.get("magic")!=MAGIC or data.get("node_id")==self.node_id: continue
            self.peers[data["node_id"]]={**data,"ip":addr[0],"last_seen":time.time()}
        s.close()

    def start(self):
        threading.Thread(target=self._broadcast_loop,daemon=True,name="naraz-discovery-beacon").start()
        threading.Thread(target=self._listen_loop,daemon=True,name="naraz-discovery-listener").start()

    def stop(self): self._stop.set()

    def snapshot(self):
        cutoff=time.time()-12
        self.peers={k:v for k,v in self.peers.items() if v.get("last_seen",0)>=cutoff}
        return [{**v,"age":round(time.time()-v["last_seen"],1)} for v in self.peers.values()]
