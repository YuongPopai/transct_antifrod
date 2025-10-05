import os, json, asyncio, threading
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from kafka import KafkaConsumer
import time as _t


BROKERS = os.getenv("KAFKA_BROKERS","kafka:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW","tx.all")
TOPIC_LEGIT = os.getenv("TOPIC_LEGIT","tx.legit")
TOPIC_SUS = os.getenv("TOPIC_SUS","tx.sus")
GROUP_BASE = os.getenv("GROUP_BASE","webui")

app = FastAPI()

html = """
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>AntiFraud Streams</title>
<style>
  body{font-family:Inter,system-ui,Arial;padding:12px;background:#0b0f1a;color:#e6edf3}
  h1{margin:0 0 12px 0;font-size:20px}
  .grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px}
  .col{background:#0f172a;border:1px solid #1f2937;border-radius:12px;overflow:auto;max-height:80vh}
  .col h2{position:sticky;top:0;background:#0f172a;margin:0;padding:10px;border-bottom:1px solid #1f2937;font-size:16px}
  .item{padding:8px 10px;border-bottom:1px dashed #233149;font-family:ui-monospace,Consolas,monaco,monospace;font-size:12px;white-space:pre-wrap}
  .green{background:rgba(16,185,129,.12);border-left:3px solid #10b981}
  .red{background:rgba(239,68,68,.12);border-left:3px solid #ef4444}
</style>
</head>
<body>
<h1>Anti‑Fraud Streams</h1>
<div class="grid">
  <div class="col"><h2>1) Raw (tx.all)</h2><div id="raw"></div></div>
  <div class="col"><h2>2) Classified: Legit</h2><div id="legit"></div></div>
  <div class="col"><h2>3) Classified: Sus</h2><div id="sus"></div></div>
</div>
<script>
function add(targetId, obj, fraud){
  const el = document.createElement('div');
  el.className = 'item ' + (fraud ? 'red':'green');
  el.textContent = JSON.stringify(obj);
  const box = document.getElementById(targetId);
  box.prepend(el);
  while (box.children.length > 300) box.removeChild(box.lastChild);
}
function ws(path, cb){
  const u = (location.protocol === 'https:' ? 'wss':'ws') + '://' + location.host + path;
  const sock = new WebSocket(u);
  sock.onmessage = ev => { try { cb(JSON.parse(ev.data)); } catch(e){} };
  sock.onclose = () => setTimeout(()=>ws(path, cb), 1200);
}
ws('/ws/raw',   m => { const ev = m.event ?? m; add('raw',   m, ev?.label==='fraud'); });
ws('/ws/legit', m => { const ev = m.event ?? m; add('legit', m, ev?.label==='fraud'); });
ws('/ws/sus',   m => { const ev = m.event ?? m; add('sus',   m, ev?.label==='fraud'); });
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(html)

class Hub:
    def __init__(self): self.clients={"raw":set(),"legit":set(),"sus":set()}
    async def send(self, key, msg):
        dead=[]
        for ws in list(self.clients[key]):
            try: await ws.send_text(json.dumps(msg, ensure_ascii=False))
            except Exception: dead.append(ws)
        for ws in dead: self.clients[key].discard(ws)
hub = Hub()

async def register(ws, key):
    await ws.accept(); hub.clients[key].add(ws)
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect:
        hub.clients[key].discard(ws)

@app.websocket("/ws/raw")
async def ws_raw(ws: WebSocket): await register(ws,"raw")

@app.websocket("/ws/legit")
async def ws_legit(ws: WebSocket): await register(ws,"legit")

@app.websocket("/ws/sus")
async def ws_sus(ws: WebSocket): await register(ws,"sus")

def start_consumer(topic, key, every_log=20):
    print(f"[web-ui] starting consumer key={key} topic={topic}", flush=True)
    cons = KafkaConsumer(
        topic,
        bootstrap_servers=BROKERS.split(","),
        group_id=f"{GROUP_BASE}-{key}-{int(_t.time())}",   # уникальная группа на каждый запуск
        auto_offset_reset="earliest",                      # читаем историю
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    loop = asyncio.get_event_loop()
    def run():
        cnt = 0
        try:
            for msg in cons:
                cnt += 1
                if cnt % every_log == 0:
                    print(f"[web-ui] consumed {cnt} from {key}", flush=True)
                asyncio.run_coroutine_threadsafe(hub.send(key, msg.value), loop)
        except Exception as e:
            print(f"[web-ui] consume error {key}: {e}", flush=True)
    threading.Thread(target=run, daemon=True).start()

@app.on_event("startup")
async def startup():
    start_consumer(os.getenv("TOPIC_RAW","tx.all"), "raw")
    start_consumer(os.getenv("TOPIC_LEGIT","tx.legit"), "legit")
    start_consumer(os.getenv("TOPIC_SUS","tx.sus"), "sus")
