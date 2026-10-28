# HopDrop

Send files and folders between laptops on the same network. Pick a device,
the receiver approves with a PIN, and the transfer streams through the host
laptop (nothing is written to its disk, so any size works) or, when that would
be slower, goes straight from browser to browser.

- **Folder, or more than 5 files** → delivered as one `.zip` (folder structure preserved).
- **Up to 5 loose files** → one download each.

## Files

- `serv.py` — the server and all the backend logic.
- `index.html` — the web page (served by `serv.py`; keep it next to `serv.py`).
- `requirements.txt` — Python dependencies.

## Run (on the host laptop)

```
pip install -r requirements.txt
python serv.py
```

It prints a URL like `http://192.168.0.40:8000/`. Open that on every laptop,
including the host. All machines must be on the same Wi-Fi/LAN.

If another laptop can't connect, allow the port through Windows Firewall once
(admin PowerShell):

```
New-NetFirewallRule -DisplayName "HopDrop 8000" -Direction Inbound -Protocol TCP -LocalPort 8000 -Action Allow
```

## Use

1. **Send** tab: pick a device, choose files or a folder, click Send.
2. The sender sees a 4-digit PIN. The receiver gets a popup and types that PIN.
3. Both sides see progress, speed, time left, and a Cancel button.
4. A folder (or more than 5 files) arrives as one `.zip`; up to 5 loose files
   arrive as separate downloads (the browser asks to allow multiple downloads once).
5. Each transfer shows whether it went **direct** or **via host**.

## Speed tips

Through the host, every byte goes sender → host laptop → receiver. When the
host is a third laptop, that crosses the network twice.

- **Direct mode (automatic).** When the host is neither the sender nor the
  receiver, the two browsers connect to each other directly over WebRTC, and
  the host only introduces them. On Wi-Fi this skips the extra hop. Browser
  WebRTC tops out around 15–20 MB/s, though, so it is only used in that
  third-machine case. If the connection can't be made within ~10 s, HopDrop
  quietly falls back to the host.
  - The receiver collects the data in the browser and saves it at the end
    (saving to disk as it arrives needs HTTPS). It therefore needs free disk
    space for about twice the transfer. Transfers over 8 GB in Chrome/Edge, or
    over 1 GB in other browsers, always go via the host.
  - If the host is plugged into Ethernet, untick "Connect straight to the
    sender" on the receiving laptop's Receive tab: via the host is faster then.
- **Run `serv.py` on the sending or receiving laptop.** One leg then stays
  inside that machine, which roughly doubles speed on Wi-Fi.
- **Plug the host into Ethernet**, or use 5 GHz Wi-Fi close to the router.
- **Many small files** are zipped automatically and stream as one download.
  Choosing their folder does the same.

## Security

There is no login — anyone on the network who opens the page can see you and
send an offer, but a transfer only completes with the PIN. Use it on a trusted
network.
