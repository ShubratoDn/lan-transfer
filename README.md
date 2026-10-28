# Healio LAN Transfer

Send files and folders between laptops on the same network. Pick a device,
the receiver approves with a PIN, and the transfer streams straight through
the host laptop — nothing is written to its disk, so any size works.

- **Folder** → delivered as one `.zip` (folder structure preserved).
- **Loose files** → one download each.

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
New-NetFirewallRule -DisplayName "Healio 8000" -Direction Inbound -Protocol TCP -LocalPort 8000 -Action Allow
```

## Use

1. **Send** tab: pick a device, choose files or a folder, click Send.
2. The sender sees a 4-digit PIN. The receiver gets a popup and types that PIN.
3. Both sides see progress, speed, time left, and a Cancel button.
4. A folder arrives as one `.zip`; several loose files arrive as separate
   downloads (the browser asks to allow multiple downloads once).

## Speed tips

Every byte goes sender → host laptop → receiver, so it crosses the network
twice. The relay itself is not the bottleneck (it handles well over 150 MB/s
on one machine); the network is.

- **Run `serv.py` on the sending or receiving laptop.** One leg then stays
  inside that machine, which roughly doubles speed on Wi-Fi.
- **Plug the host into Ethernet**, or use 5 GHz Wi-Fi close to the router.
- **Send many files as a folder.** It streams as one continuous zip instead of
  one download per file.

## Security

There is no login — anyone on the network who opens the page can see you and
send an offer, but a transfer only completes with the PIN. Use it on a trusted
network.
