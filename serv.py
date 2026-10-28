"""HopDrop: send files/folders to a chosen device on the network.

Run:  pip install -r requirements.txt
      python serv.py
Open the printed URL on every laptop (including this one). Each open page shows
up as a device; pick one, choose files/folders, and the receiver must accept
with the PIN shown on the sender's screen.

Files go straight from browser to browser over WebRTC when possible; this laptop
then only introduces the two. Otherwise they are relayed through this laptop and
never written to its disk, so any size works regardless of free space here.
A folder (or more than a few files) is delivered as one zip built on the fly.
"""
import asyncio
import re
import secrets
import socket
import time
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.responses import HTMLResponse, StreamingResponse

PORT = 8000
QDEPTH = 32          # chunks buffered each side of the relay for backpressure
LOOSE_MAX = 5        # more loose files than this are delivered as one zip
MAX_TRIES = 3        # wrong PIN attempts before the offer is dropped
GRACE = 15           # seconds a device may be offline before its transfers are cancelled
KEEP = 60            # seconds finished transfers stay on the server
READY_WAIT = 300     # seconds to wait for the receiver to start downloading
DIRECT_WAIT = 25     # seconds a direct connection may take to deliver data before falling back to the relay
ACTIVE = {"offered", "accepted", "ready", "downloading"}


class Peer:
    def __init__(self, pid: str):
        self.id = pid
        self.name = ""
        self.ws: WebSocket | None = None
        self.ip = ""
        self.off = time.time()


class Transfer:
    def __init__(self, frm: str, to: str, files: list, ref: str):
        self.id = secrets.token_urlsafe(6)
        self.frm, self.to, self.ref, self.files = frm, to, ref, files
        self.total = sum(f["size"] for f in files)
        self.pin = f"{secrets.randbelow(10000):04d}"
        self.skey = secrets.token_urlsafe(12)   # sender's upload key
        self.rkey = secrets.token_urlsafe(12)   # receiver's download key
        self.state, self.note, self.tries = "offered", "", 0
        self.relay = True                       # False while the browsers talk directly (WebRTC)
        self.up = self.down = 0
        self.sp_up = self.sp_down = 0.0
        self.last = (time.time(), 0, 0)
        self.ended: float | None = None
        self.dirty = False
        self.done = 0                           # files fully delivered to the receiver
        self.started = False                    # receiver has begun downloading
        # A folder (any path with "/") or many loose files is delivered as one streaming zip.
        self.zip = len(files) > LOOSE_MAX or any("/" in f["name"] for f in files)
        self.out_name = zip_name(self) if self.zip else ""
        self.zf: zipfile.ZipFile | None = None  # zip mode: writer feeding zbuf
        self.zbuf: list[bytes] = []             # zip mode: bytes written but not yet queued
        self.next_up = 0                        # zip mode: index of the file the zip expects next
        self.up_busy = False
        self.out_q: asyncio.Queue | None = None # zip mode: zip bytes -> receiver
        self.qs: dict[int, asyncio.Queue] = {}  # file mode: one relay queue per file
        self.since = time.time()

    def q(self, i: int) -> asyncio.Queue:
        if i not in self.qs:
            self.qs[i] = asyncio.Queue(maxsize=QDEPTH)
        return self.qs[i]


peers: dict[str, Peer] = {}
transfers: dict[str, Transfer] = {}


def get_lan_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def host_ips() -> set[str]:
    ips = {"127.0.0.1", "::1", get_lan_ip()}
    try:
        ips.update(socket.gethostbyname_ex(socket.gethostname())[2])
    except OSError:
        pass
    return ips


LOCAL_IPS = host_ips()


def worth_direct(t: Transfer) -> bool:
    """Browser-to-browser only pays off when the relay costs two network hops, i.e. when this
    laptop is neither sender nor receiver. WebRTC is also slower per hop than plain HTTP."""
    return all(peers[pid].ip not in LOCAL_IPS for pid in (t.frm, t.to) if pid in peers)


def clean_rel(name) -> str:
    """Make a client-supplied relative path safe (no .., drive letters, odd chars)."""
    parts = [p for p in re.split(r"[\\/]+", str(name)) if p not in ("", ".", "..")]
    return "/".join(re.sub(r'[<>:"|?*\x00-\x1f]', "_", p)[:200] for p in parts)


def uniq_name(pid: str, name: str, fallback: str = "Device") -> str:
    base = re.sub(r"\s+", " ", str(name or "")).strip()[:30] or fallback
    taken = {q.name for q in peers.values() if q.ws and q.id != pid}
    nm, k = base, 2
    while nm in taken:
        nm, k = f"{base} ({k})", k + 1
    return nm


def pname(pid: str) -> str:
    p = peers.get(pid)
    return p.name if p else "?"


def zip_name(t: Transfer) -> str:
    """A friendly download name for the single zip a folder transfer produces."""
    tops = {f["name"].split("/")[0] for f in t.files}
    nested = all("/" in f["name"] for f in t.files)
    return (tops.pop() if nested and len(tops) == 1 else f"{pname(t.frm)}-files") + ".zip"


def summary(t: Transfer, viewer: str) -> dict:
    d = dict(
        id=t.id, ref=t.ref, frm=t.frm, to=t.to, frm_name=pname(t.frm), to_name=pname(t.to),
        state=t.state, note=t.note, relay=t.relay, zip=t.zip, out_name=t.out_name,
        count=len(t.files), total=t.total, done=t.done, up=t.up, down=t.down,
        sp_up=round(t.sp_up), sp_down=round(t.sp_down),
        names=[f["name"] for f in t.files[:6]],
    )
    if viewer == t.frm:
        d["key"] = t.skey
        if t.state == "offered":
            d["pin"] = t.pin
    elif viewer == t.to:
        d["key"] = t.rkey
    return d


async def send(pid: str, msg: dict):
    p = peers.get(pid)
    if p and p.ws:
        try:
            await p.ws.send_json(msg)
        except Exception:
            pass


async def push(t: Transfer):
    t.dirty = False
    for pid in {t.frm, t.to}:
        await send(pid, {"type": "transfer", "t": summary(t, pid)})


async def broadcast_peers():
    online = [q for q in peers.values() if q.ws]
    for p in online:
        await send(p.id, {
            "type": "peers", "me": p.id, "me_name": p.name,
            "peers": [{"id": q.id, "name": q.name} for q in online if q.id != p.id],
        })


def finish(t: Transfer, state: str, note: str = ""):
    if t.state not in ACTIVE:
        return
    t.state, t.note, t.ended = state, note, time.time()
    t.sp_up = t.sp_down = 0.0
    t.dirty = True
    # Nudge any waiting relay handlers so they notice the state change and exit.
    for q in (t.out_q, *t.qs.values()):
        if q is not None:
            try:
                q.put_nowait(None)
            except asyncio.QueueFull:
                pass


async def end(t: Transfer, state: str, note: str = ""):
    finish(t, state, note)
    await push(t)


class ZipSink:
    """Non-seekable target, so zipfile emits streaming data descriptors; bytes collect in t.zbuf."""
    def __init__(self, t: Transfer):
        self.t = t

    def write(self, b):
        self.t.zbuf.append(bytes(b))
        return len(b)

    def flush(self):
        pass


def use_relay(t: Transfer):
    """Switch an accepted transfer to the relay through this laptop."""
    t.relay, t.state, t.since = True, "ready", time.time()
    t.up = t.down = t.done = 0
    t.sp_up = t.sp_down = 0.0
    if t.zip:
        t.zbuf.clear()
        t.zf = zipfile.ZipFile(ZipSink(t), "w", zipfile.ZIP_STORED, allowZip64=True)
        t.out_q = asyncio.Queue(maxsize=QDEPTH)


def as_int(v, hi: int) -> int | None:
    return min(v, hi) if isinstance(v, int) and not isinstance(v, bool) and v >= 0 else None


async def handle(p: Peer, m: dict):
    ty = m.get("type")
    if ty == "rename":
        p.name = uniq_name(p.id, m.get("name"), p.name or "Device")
        await broadcast_peers()
        return
    if ty == "offer":
        to = peers.get(m.get("to"))
        files = []
        for f in (m.get("files") or [])[:20000]:
            name, size = clean_rel(f.get("name", "")), f.get("size")
            if name and isinstance(size, int) and size >= 0:
                files.append({"name": name, "size": size})
        if not to or not to.ws or to.id == p.id or not files:
            await send(p.id, {"type": "error", "msg": "That device is no longer available."})
            return
        t = Transfer(p.id, to.id, files, str(m.get("ref", ""))[:40])
        transfers[t.id] = t
        await push(t)
        return
    t = transfers.get(m.get("tid"))
    if not t or p.id not in (t.frm, t.to):
        return
    direct = not t.relay and t.state in ("accepted", "downloading")
    if ty == "accept" and p.id == t.to and t.state == "offered":
        if secrets.compare_digest(str(m.get("pin", "")).strip(), t.pin):
            if m.get("direct") is True and worth_direct(t):   # sender starts WebRTC
                t.relay, t.state, t.since = False, "accepted", time.time()
            else:
                use_relay(t)
            await push(t)
        else:
            t.tries += 1
            if t.tries >= MAX_TRIES:
                await end(t, "declined", "Too many wrong PINs")
            else:
                await send(p.id, {"type": "pin_bad", "tid": t.id, "left": MAX_TRIES - t.tries})
    elif ty == "decline" and p.id == t.to and t.state == "offered":
        await end(t, "declined", f"{p.name} declined")
    elif ty == "cancel" and t.state in ACTIVE:
        why = str(m.get("reason", ""))[:60]
        await end(t, "cancelled", f"Cancelled by {p.name}" + (f" ({why})" if why else ""))
    elif ty == "signal" and direct and isinstance(m.get("data"), dict):
        # WebRTC offer/answer/ICE candidates, passed between the two browsers only.
        await send(t.to if p.id == t.frm else t.frm, {"type": "signal", "tid": t.id, "data": m["data"]})
    elif ty == "progress" and direct:
        # In direct mode the bytes never pass through here, so each side reports its own count.
        if p.id == t.frm:
            up = as_int(m.get("up"), t.total)
            if up is not None:
                t.up = max(t.up, up)
        else:
            down, done = as_int(m.get("down"), t.total), as_int(m.get("done"), len(t.files))
            if down is not None:
                t.down = max(t.down, down)
            if done is not None:
                t.done = max(t.done, done)
            if t.state == "accepted" and t.down > 0:
                t.state, t.started, t.dirty = "downloading", True, True
    elif ty == "complete" and direct and p.id == t.to:
        t.up, t.down, t.done = t.total, t.total, len(t.files)
        await end(t, "done")
    elif ty == "fallback" and direct and t.state == "accepted":
        use_relay(t)
        await push(t)


async def ticker():
    """Every 0.5s: compute speeds, push progress, drop stale transfers/devices."""
    while True:
        await asyncio.sleep(0.5)
        now = time.time()
        for t in list(transfers.values()):
            if t.state in ACTIVE:
                t0, up0, down0 = t.last
                dt = max(now - t0, 1e-6)
                for attr, cur, prev in (("sp_up", t.up, up0), ("sp_down", t.down, down0)):
                    inst = (cur - prev) / dt
                    old = getattr(t, attr)
                    setattr(t, attr, inst if old == 0 or inst == 0 else 0.6 * old + 0.4 * inst)
                t.last = (now, t.up, t.down)
                for pid in (t.frm, t.to):
                    q = peers.get(pid)
                    if not q or (q.ws is None and now - q.off > GRACE):
                        finish(t, "cancelled", "A device went offline")
                        break
                if t.state == "ready" and now - t.since > READY_WAIT:
                    finish(t, "cancelled", "Receiver never started the download")
                if t.state == "accepted" and not t.relay and now - t.since > DIRECT_WAIT:
                    use_relay(t)
                    t.dirty = True
                if t.state in ("ready", "downloading") or t.dirty:
                    await push(t)
            elif t.dirty:
                await push(t)
            if t.ended and now - t.ended > KEEP:
                transfers.pop(t.id, None)
        busy = {x for t in transfers.values() for x in (t.frm, t.to)}
        for pid, q in list(peers.items()):
            if q.ws is None and now - q.off > 60 and pid not in busy:
                del peers[pid]


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(ticker())
    yield
    task.cancel()


app = FastAPI(title="HopDrop", lifespan=lifespan)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket, id: str, name: str = ""):
    await ws.accept()
    pid = id[:64]
    p = peers.get(pid) or Peer(pid)
    peers[pid] = p
    old, p.ws = p.ws, ws
    p.ip = ws.client.host if ws.client else ""
    if old:
        try:
            await old.close()
        except Exception:
            pass
    p.name = uniq_name(pid, name or p.name)
    await broadcast_peers()
    for t in list(transfers.values()):
        if pid in (t.frm, t.to):
            await send(pid, {"type": "transfer", "t": summary(t, pid)})
    try:
        while True:
            await handle(p, await ws.receive_json())
    except Exception:
        pass
    finally:
        if p.ws is ws:
            p.ws, p.off = None, time.time()
            await broadcast_peers()


_WAIT = object()


async def qput(t: Transfer, q: asyncio.Queue, item):
    """Hand an item to a relay queue, giving up if the transfer ends."""
    while t.state in ACTIVE:
        if not q.full():
            q.put_nowait(item)
            return
        try:
            await asyncio.wait_for(q.put(item), 0.5)
            return
        except asyncio.TimeoutError:
            pass
    raise HTTPException(409, "Cancelled")


async def qget(q: asyncio.Queue):
    """Next item from a relay queue, or _WAIT after 0.5s so the caller can re-check the state."""
    if not q.empty():
        return q.get_nowait()
    try:
        return await asyncio.wait_for(q.get(), 0.5)
    except asyncio.TimeoutError:
        return _WAIT


async def flush_zip(t: Transfer):
    if t.zbuf:
        data = b"".join(t.zbuf)
        t.zbuf.clear()
        await qput(t, t.out_q, data)


@app.post("/api/up/{tid}/{i}")
async def upload(tid: str, i: int, key: str, request: Request):
    """Stream one file's bytes from the sender: into the zip, or into that file's queue."""
    t = transfers.get(tid)
    if not t or not secrets.compare_digest(key, t.skey):
        raise HTTPException(404, "Unknown transfer")
    if not t.relay or t.state not in ("ready", "downloading") or not 0 <= i < len(t.files):
        raise HTTPException(409, "Transfer not accepting data")
    if t.zip and (t.up_busy or i != t.next_up):
        raise HTTPException(409, "Files must arrive in order")   # zip entries are written in sequence
    try:
        if t.zip:
            t.up_busy = True
            f = t.files[i]
            info = zipfile.ZipInfo(f["name"], time.localtime()[:6])
            info.file_size = f["size"]                          # lets zipfile skip ZIP64 for small files
            entry = t.zf.open(info, "w")
            async for chunk in request.stream():
                if t.state not in ACTIVE:
                    raise HTTPException(409, "Cancelled")
                t.up += len(chunk)
                entry.write(chunk)
                await flush_zip(t)
            entry.close()
            t.next_up += 1
            if t.next_up == len(t.files):
                t.zf.close()                                    # writes the central directory
            await flush_zip(t)
            if t.next_up == len(t.files):
                await qput(t, t.out_q, None)                    # end of the zip
        else:
            q = t.q(i)
            async for chunk in request.stream():
                if t.state not in ACTIVE:
                    raise HTTPException(409, "Cancelled")
                t.up += len(chunk)
                await qput(t, q, chunk)
            await qput(t, q, None)                              # end-of-file marker
    except HTTPException:
        raise
    except Exception:
        await end(t, "cancelled", "Sender connection lost")
        raise HTTPException(499, "Sender connection lost")
    finally:
        t.up_busy = False
    return {"ok": True}


@app.get("/api/down/{tid}/{i}")
async def download(tid: str, i: int, key: str):
    """Receiver pull: the whole folder as one zip, or a single loose file."""
    t = transfers.get(tid)
    if not t or not secrets.compare_digest(key, t.rkey):
        raise HTTPException(404, "Unknown transfer")
    if not t.relay or t.state not in ("ready", "downloading") or not 0 <= i < len(t.files):
        raise HTTPException(409, "Not ready")
    if t.zip and t.started:
        raise HTTPException(409, "Already downloading")         # a second reader would split the zip
    if not t.started:
        t.state, t.started, t.since = "downloading", True, time.time()
        await push(t)

    if t.zip:
        async def gen():
            done = False
            try:
                while t.state in ACTIVE:
                    b = await qget(t.out_q)
                    if b is _WAIT:
                        continue
                    if b is None:
                        done = True
                        break
                    t.down += len(b)
                    yield b
            finally:
                if done:
                    t.done = len(t.files)
                    finish(t, "done")
                elif t.state in ACTIVE:
                    finish(t, "cancelled", "Download interrupted")

        fn, size = t.out_name, None               # zip length is not known ahead of time -> chunked
    else:
        q, size = t.q(i), t.files[i]["size"]

        async def gen():
            got = 0
            try:
                while t.state in ACTIVE:
                    b = await qget(q)
                    if b is _WAIT:
                        continue
                    if b is None:
                        break
                    got += len(b)
                    t.down += len(b)
                    yield b
            finally:
                if got >= size:                   # this file arrived in full
                    t.done += 1
                    t.qs.pop(i, None)
                    if t.done >= len(t.files):
                        finish(t, "done")
                    else:
                        t.dirty = True            # tell the receiver to fetch the next file...
                        await push(t)             # ...now, not on the next 0.5s tick
                elif t.state in ACTIVE:           # a streamed file cannot be resumed
                    finish(t, "cancelled", "Download interrupted")

        fn = t.files[i]["name"].replace("/", "__")  # keep a hint of structure in a flat filename

    disp = f"attachment; filename=\"{re.sub(r'[^A-Za-z0-9._-]', '_', fn)}\"; filename*=UTF-8''{quote(fn)}"
    headers = {"Content-Disposition": disp}
    if size is not None:
        headers["Content-Length"] = str(size)
    return StreamingResponse(gen(), media_type="application/octet-stream", headers=headers)


@app.get("/api/info")
def info():
    return {"url": f"http://{get_lan_ip()}:{PORT}"}


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE_FILE.read_text(encoding="utf-8")


PAGE_FILE = Path(__file__).with_name("index.html")


if __name__ == "__main__":
    try:
        import websockets  # noqa: F401
    except ImportError:
        try:
            import wsproto  # noqa: F401
        except ImportError:
            raise SystemExit("WebSocket support missing. Run:  pip install -r requirements.txt")
    print(f"\nOpen on every laptop: http://{get_lan_ip()}:{PORT}/\n")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")
