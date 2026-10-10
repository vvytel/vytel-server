# Vytel's Academy Chronicles - game server

The online server for Vytel's Academy Chronicles: it relays co-op games (rooms with a 6-letter code, up to 4 players,
reconnects, an Open Games list) and tells the launcher which game version to download.

* `server_v2.py` - the whole server (Python 3.8+, standard library only). `python server_v2.py --help` lists the options.
* `render.yaml` - one-click setup on Render (free web service).
* `Dockerfile` - for any Docker host. `vytel-server.service` - for an always-on Linux VM.

Endpoints: `/ws` (game connection), `/healthz` (health check), `/rooms` (open games), `/builds/version.json` (launcher).

Game downloads are NOT stored here: they are GitHub Release files of this repository (fast, free, no traffic limit).
