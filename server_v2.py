"""Vytel's Academy Chronicles game server, version 2.

It does three things:
  1. Relays online play over WebSockets (/ws): a host and up to 3 guests join one room by a 6-letter code. Nobody
     connects to anybody directly, so routers, NAT and firewalls are never a problem.
  2. Serves the launcher's update files: /builds/version.json and the game zip (with resume support).
  3. Serves the browser version of the game (index.html + js/) when those files are next to it (optional).

Run it on your own PC or on a free cloud host:

    python server_v2.py                 # port 8000 (or the PORT environment variable on cloud hosts)
    python server_v2.py 8080            # another port
    python server_v2.py --help          # all options

Pure standard library (Python 3.8+) - nothing to install.

Relay protocol (version 2 is a superset of version 1, so older game builds keep working):
  host  -> {"t":"rhost"[,"v":2,"public":true,"name":"..","mode":".."]}
        <- {"t":"rcode","code":"ABC123","public":"https://..","token":"..","v":2}
  guest -> {"t":"rjoin","code":"ABC123"[,"v":2]}          <- {"t":"ropen","slot":1..3,"token":".."}
           the host gets {"t":"rjoin","slot":n}
  host messages go to every guest (or one, with "_to"); guest messages reach the host tagged "_f" (their slot)
  {"t":"rleave","slot":n[,"reason":..]} (host: a guest left)    {"t":"rclose"[,"reason":..]} (guest: the host left)
  {"t":"rerr","error":"peer-unavailable"|"full"|"server-full"|"rate-limited"|"room-gone"|"kicked"|...}
 Version 2 additions (only sent to clients that said "v":2, or as answers to requests only v2 clients make):
  reconnect   host  -> {"t":"rhost","v":2,"resume":"ABC123","token":".."}  <- rcode with "resumed":true + {"t":"rpeers","slots":[..]}
              guest -> {"t":"rjoin","v":2,"code":"ABC123","token":".."}   <- ropen with "resumed":true (same slot)
              A v2 client whose connection drops (no close frame) keeps its room / slot for GRACE seconds (default 60).
              While a v2 host is away, v2 guests get {"t":"rhostwait","secs":60}, then {"t":"rhostback"} or rclose.
  lobby list  anyone -> {"t":"rlist"}  <- {"t":"rrooms","rooms":[{"code","name","mode","players","max","state","age"}]}
              host   -> {"t":"rmeta","public":true,"name":"..","mode":"story","state":"lobby"|"playing"}
              also over HTTP: GET /rooms
  kick        host   -> {"t":"rkick","slot":n}
  heartbeat   WebSocket ping frames every PING_INTERVAL seconds (every client answers them automatically).
  compression permessage-deflate (RFC 7692, no context takeover) when the client offers it.
"""
import argparse
import base64
import hashlib
import json
import logging
import mimetypes
import os
import queue
import random
import secrets
import signal
import socket
import struct
import sys
import threading
import time
import urllib.request
import zlib
from email.utils import formatdate
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

SERVER_VERSION = '2.0'
ROOT = os.path.dirname(os.path.abspath(__file__))
GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
MAX_GUESTS = 3
CONTROL = ('rmeta', 'rlist', 'rkick')
log = logging.getLogger('vytel')


def env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def env_bool(name, default):
    v = os.environ.get(name)
    return default if v is None else v.strip().lower() in ('1', 'true', 'yes', 'on')


class Cfg:
    """Every setting comes from a command-line flag, else an environment variable, else the default."""
    port = 8000
    host = '0.0.0.0'
    public_url = None
    builds_dir = os.path.join(ROOT, 'builds')
    version_url = None           # where to fetch version.json when builds/ has none (e.g. a GitHub Release asset)
    web_dir = ROOT
    serve_web = True
    max_rooms = 100
    max_clients = 400
    max_conns_per_ip = 16
    max_rooms_per_ip = 4
    new_conns_per_min = 60       # per IP
    bad_joins_per_min = 12       # per IP: wrong codes (stops code guessing)
    msg_rate = 200               # messages per second per connection (game traffic is ~20-60)
    byte_rate = 4 << 20          # bytes per second per connection
    max_msg = 1 << 20            # biggest single message
    max_queue = 8 << 20          # bytes waiting for a slow receiver before it is dropped
    ping_interval = 15
    ping_timeout = 45            # nothing heard for this long = connection is dead
    idle_timeout = 900           # connected but not in a room for this long = closed
    host_grace = 60              # a dropped v2 host keeps its room this long
    guest_grace = 60             # a dropped v2 guest keeps its slot this long
    lobby_default = False        # list rooms of old clients (that can't say "public") in the lobby list?
    trust_proxy = None           # use X-Forwarded-For (None = auto)
    compress = True
    log_level = 'INFO'


def load_config(argv=None):
    p = argparse.ArgumentParser(description="Vytel's Academy Chronicles game server (relay + launcher updates + web game).")
    p.add_argument('port', nargs='?', type=int, help='port to listen on (default: $PORT or 8000)')
    p.add_argument('--host', help='address to bind (default 0.0.0.0 = every network interface)')
    p.add_argument('--public-url', help='the address friends use, e.g. https://vytel.onrender.com (shown in the lobby)')
    p.add_argument('--builds-dir', help='folder with version.json and the game zips (default: ./builds)')
    p.add_argument('--version-url', help='fetch version.json from this URL when builds/ has none, e.g. '
                   'https://github.com/NAME/REPO/releases/latest/download/version.json')
    p.add_argument('--no-web', action='store_true', help="don't serve the browser game files")
    p.add_argument('--max-rooms', type=int)
    p.add_argument('--lobby-all', action='store_true', help='list every room in the lobby list, not only rooms marked public')
    p.add_argument('--log-level', help='DEBUG, INFO, WARNING')
    a = p.parse_args(argv)
    c = Cfg
    c.port = a.port or env_int('PORT', 8000)
    c.host = a.host or os.environ.get('HOST', '0.0.0.0')
    c.builds_dir = os.path.abspath(a.builds_dir or os.environ.get('BUILDS_DIR', c.builds_dir))
    c.version_url = a.version_url or os.environ.get('VERSION_URL') or None
    c.web_dir = os.path.abspath(os.environ.get('WEB_DIR', ROOT))
    c.serve_web = not a.no_web and env_bool('SERVE_WEB', True) and os.path.exists(os.path.join(c.web_dir, 'index.html'))
    c.max_rooms = a.max_rooms or env_int('MAX_ROOMS', c.max_rooms)
    c.max_clients = env_int('MAX_CLIENTS', c.max_clients)
    c.max_conns_per_ip = env_int('MAX_CONNS_PER_IP', c.max_conns_per_ip)
    c.max_rooms_per_ip = env_int('MAX_ROOMS_PER_IP', c.max_rooms_per_ip)
    c.new_conns_per_min = env_int('NEW_CONNS_PER_MIN', c.new_conns_per_min)
    c.bad_joins_per_min = env_int('BAD_JOINS_PER_MIN', c.bad_joins_per_min)
    c.msg_rate = env_int('MSG_RATE', c.msg_rate)
    c.ping_interval = env_int('PING_INTERVAL', c.ping_interval)
    c.ping_timeout = env_int('PING_TIMEOUT', c.ping_timeout)
    c.idle_timeout = env_int('IDLE_TIMEOUT', c.idle_timeout)
    c.host_grace = env_int('HOST_GRACE', c.host_grace)
    c.guest_grace = env_int('GUEST_GRACE', c.guest_grace)
    c.lobby_default = a.lobby_all or env_bool('LOBBY_ALL', False)
    c.compress = env_bool('COMPRESS', True)
    tp = os.environ.get('TRUST_PROXY')
    c.trust_proxy = None if tp is None else env_bool('TRUST_PROXY', False)
    c.log_level = (a.log_level or os.environ.get('LOG_LEVEL', 'INFO')).upper()
    c.public_url = (a.public_url or os.environ.get('PUBLIC_URL') or cloud_url() or '').rstrip('/') or None
    return c


def cloud_url():
    """The service's public https address on common hosts (they set these variables themselves)."""
    e = os.environ
    if e.get('RENDER_EXTERNAL_URL'):
        return e['RENDER_EXTERNAL_URL']
    if e.get('SPACE_HOST'):                      # Hugging Face Spaces
        return 'https://' + e['SPACE_HOST']
    if e.get('KOYEB_PUBLIC_DOMAIN'):
        return 'https://' + e['KOYEB_PUBLIC_DOMAIN']
    if e.get('RAILWAY_PUBLIC_DOMAIN'):
        return 'https://' + e['RAILWAY_PUBLIC_DOMAIN']
    if e.get('FLY_APP_NAME'):
        return 'https://%s.fly.dev' % e['FLY_APP_NAME']
    return None


ON_CLOUD = bool(cloud_url())


# ------------------------------------------------------------------------------------------------ rate limiting
class Bucket:
    """Token bucket: allow(n) is False when more than `rate` per second (with `burst`) are used."""
    __slots__ = ('rate', 'burst', 'tokens', 'at')

    def __init__(self, rate, burst):
        self.rate, self.burst, self.tokens, self.at = rate, burst, burst, time.monotonic()

    def allow(self, n=1):
        now = time.monotonic()
        self.tokens = min(self.burst, self.tokens + (now - self.at) * self.rate)
        self.at = now
        if self.tokens >= n:
            self.tokens -= n
            return True
        return False


class IpTable:
    """Per-IP counters: open connections, new connections per minute, failed joins per minute."""

    def __init__(self):
        self.lock = threading.Lock()
        self.conns = {}
        self.new = {}
        self.bad = {}

    def _bucket(self, table, ip, per_min):
        b = table.get(ip)
        if b is None:
            b = table[ip] = Bucket(per_min / 60.0, per_min)
        return b

    def admit(self, ip):
        with self.lock:
            if self.conns.get(ip, 0) >= Cfg.max_conns_per_ip:
                return 'too many connections from your address'
            if not self._bucket(self.new, ip, Cfg.new_conns_per_min).allow():
                return 'too many new connections - slow down'
            self.conns[ip] = self.conns.get(ip, 0) + 1
            return None

    def release(self, ip):
        with self.lock:
            n = self.conns.get(ip, 0) - 1
            if n > 0:
                self.conns[ip] = n
            else:
                self.conns.pop(ip, None)

    def bad_join(self, ip):
        """Count a failed join; True when this address has guessed too many wrong codes."""
        with self.lock:
            return not self._bucket(self.bad, ip, Cfg.bad_joins_per_min).allow()

    def prune(self):
        with self.lock:
            for table in (self.new, self.bad):
                for ip in [ip for ip, b in table.items() if time.monotonic() - b.at > 600]:
                    del table[ip]


ips = IpTable()


# ------------------------------------------------------------------------------------------------ WebSocket connection
def make_frame(op, data, rsv1=False):
    n = len(data)
    b1 = 0x80 | op | (0x40 if rsv1 else 0)
    if n < 126:
        header = struct.pack('>BB', b1, n)
    elif n < 65536:
        header = struct.pack('>BBH', b1, 126, n)
    else:
        header = struct.pack('>BBQ', b1, 127, n)
    return header + data


class Outgoing:
    """One text message prepared once (plain and compressed) so a broadcast compresses it a single time."""
    __slots__ = ('text', '_plain', '_z')

    def __init__(self, text):
        self.text = text
        self._plain = self._z = None

    def frame(self, deflate):
        if deflate and len(self.text) >= 160:
            if self._z is None:
                c = zlib.compressobj(6, zlib.DEFLATED, -15)
                z = c.compress(self.text.encode('utf-8')) + c.flush(zlib.Z_SYNC_FLUSH)
                self._z = make_frame(0x1, z[:-4] if z.endswith(b'\x00\x00\xff\xff') else z, rsv1=True)
            return self._z
        if self._plain is None:
            self._plain = make_frame(0x1, self.text.encode('utf-8'))
        return self._plain


class Conn:
    """A server-side RFC 6455 WebSocket: the handler thread reads, a writer thread sends from a queue so one slow
    receiver can never stall the sender (it is dropped instead when its queue grows too big)."""
    _ids = 0

    def __init__(self, sock, prefix, ip, deflate):
        Conn._ids += 1
        self.id = Conn._ids
        self.ip, self.deflate = ip, deflate
        # Reads wake up every second (so a killed connection's thread always ends, even on Windows where shutting a
        # socket down doesn't interrupt a blocked recv); sends get their own handle with a longer timeout.
        self.sock = sock
        self.sock.settimeout(1.0)
        self.wsock = sock.dup()
        self.wsock.settimeout(20.0)
        self.buf = bytearray(prefix)
        self.alive = True
        self.got_close = False         # the client said goodbye properly (close frame)
        self.replaced = False          # a reconnect took over this connection's room/slot
        self.created = self.last_rx = time.monotonic()
        self.q = queue.Queue()
        self.queued = 0
        self.qlock = threading.Lock()
        self.rate = Bucket(Cfg.msg_rate, Cfg.msg_rate * 2)
        self.brate = Bucket(Cfg.byte_rate, Cfg.byte_rate * 2)
        self.inflater = None
        self.drops = 0
        self.last_ping = self.created
        self.v = 1
        self.room = None
        self.role = None               # 'host' or a guest slot (1-3)
        self.bytes_in = self.bytes_out = 0
        self.writer = threading.Thread(target=self._write_loop, daemon=True, name='ws-write-%d' % self.id)
        self.writer.start()

    # ---- sending
    def _enqueue(self, data):
        if not self.alive:
            return
        with self.qlock:
            self.queued += len(data)
            too_much = self.queued > Cfg.max_queue
        if too_much:
            log.warning('dropping %s: it is not keeping up (%d KB queued)', self.label(), self.queued >> 10)
            self.kill()
            return
        self.q.put(data)

    def _write_loop(self):
        while True:
            data = self.q.get()
            if data is None:
                break
            with self.qlock:
                self.queued -= len(data)
            try:
                self.wsock.sendall(data)
                self.bytes_out += len(data)
                Stats.bytes_out += len(data)
            except OSError:
                self.kill()
                break

    def send(self, obj_or_out):
        out = obj_or_out if isinstance(obj_or_out, Outgoing) else Outgoing(json.dumps(obj_or_out, separators=(',', ':')))
        self._enqueue(out.frame(self.deflate))

    def ping(self):
        self._enqueue(make_frame(0x9, b'vy'))

    def close(self, code=1000, reason=''):
        if self.alive:
            self._enqueue(make_frame(0x8, struct.pack('>H', code) + reason.encode()[:100]))
            self.q.put(None)
            self.alive = False
            threading.Timer(2.0, self._shutdown).start()

    def kill(self):
        """Drop the connection now (dead peer, flooding, slow receiver). The reader returns None right away."""
        self.alive = False
        self.q.put(None)
        self._shutdown()

    def _shutdown(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        for s in (self.wsock, self.sock):
            try:
                s.close()
            except OSError:
                pass

    # ---- receiving
    def _read(self, n):
        while len(self.buf) < n:
            try:
                chunk = self.sock.recv(max(65536, n - len(self.buf)))
            except socket.timeout:
                if not self.alive:
                    raise ConnectionError('closed')
                continue
            if not chunk:
                raise ConnectionError('closed')
            self.buf += chunk
        data = bytes(self.buf[:n])
        del self.buf[:n]
        return data

    def recv(self):
        """The next text message, or None when the connection is over."""
        parts, compressed, total = [], False, 0
        try:
            while True:
                b1, b2 = self._read(2)
                fin, op, rsv1 = b1 & 0x80, b1 & 0x0F, b1 & 0x40
                length = b2 & 0x7F
                if length == 126:
                    length = struct.unpack('>H', self._read(2))[0]
                elif length == 127:
                    length = struct.unpack('>Q', self._read(8))[0]
                if length > Cfg.max_msg:
                    log.warning('%s sent a %d byte frame - closing', self.label(), length)
                    self.close(1009, 'message too big')
                    return None
                mask = self._read(4) if b2 & 0x80 else b'\0\0\0\0'
                payload = self._read(length) if length else b''
                if length:
                    # XOR with the 4-byte mask, fast: one big-int operation instead of a Python loop per byte.
                    m = (mask * (length // 4 + 1))[:length]
                    payload = (int.from_bytes(payload, 'big') ^ int.from_bytes(m, 'big')).to_bytes(length, 'big')
                self.last_rx = time.monotonic()
                self.bytes_in += length + 6
                Stats.bytes_in += length + 6
                if op == 0x8:                       # close
                    self.got_close = True
                    return None
                if op == 0x9:                       # ping -> pong
                    self._enqueue(make_frame(0xA, payload[:125]))
                    continue
                if op == 0xA:                       # pong
                    continue
                if op in (0x1, 0x2):
                    parts, compressed, total = [], bool(rsv1) and self.deflate, 0
                total += length
                if total > Cfg.max_msg:
                    self.close(1009, 'message too big')
                    return None
                parts.append(payload)
                if not fin:
                    continue
                data = b''.join(parts)
                if compressed:
                    data = self._inflate(data)
                    if data is None:
                        return None
                if not self.rate.allow() or not self.brate.allow(len(data)):
                    Stats.dropped += 1             # over the limit: drop this message
                    self.drops += 1
                    if self.drops > Cfg.msg_rate * 10:
                        log.warning('%s is flooding the server - closing it', self.label())
                        self.close(1008, 'too many messages')
                        return None
                    continue
                self.drops = 0
                return data.decode('utf-8', 'replace')
        except (ConnectionError, OSError, ValueError, struct.error):
            return None

    def _inflate(self, data):
        if self.inflater is None or self.inflater.eof:
            self.inflater = zlib.decompressobj(-15)
        try:
            out = self.inflater.decompress(data + b'\x00\x00\xff\xff', Cfg.max_msg + 1)
        except zlib.error:
            self.close(1007, 'bad compressed data')
            return None
        if len(out) > Cfg.max_msg or self.inflater.unconsumed_tail:
            self.close(1009, 'message too big')
            return None
        return out

    def label(self):
        return '%s#%d(%s)' % ('host' if self.role == 'host' else 'guest%s' % self.role if self.role else 'client', self.id, self.ip)


# ------------------------------------------------------------------------------------------------ rooms
class Room:
    def __init__(self, code, host):
        self.code = code
        self.host = host
        self.host_token = secrets.token_urlsafe(12)
        self.host_v = host.v
        self.host_ip = host.ip
        self.host_away_until = None      # set while a dropped v2 host may still come back
        self.guests = {}                 # slot -> Conn (None while a dropped v2 guest may come back)
        self.tokens = {}                 # slot -> resume token
        self.away = {}                   # slot -> deadline
        self.public = Cfg.lobby_default
        self.name = ''
        self.mode = ''
        self.state = 'lobby'
        self.created = time.time()
        self.bytes = 0

    def players(self):
        return 1 + len(self.guests)     # a slot held for a dropped guest still counts

    def listing(self):
        return {'code': self.code, 'name': self.name or 'Game %s' % self.code[:3], 'mode': self.mode, 'state': self.state,
                'players': self.players(), 'max': MAX_GUESTS + 1, 'age': int(time.time() - self.created),
                'away': self.host is None}


rooms = {}
rooms_lock = threading.RLock()
clients = set()


class Stats:
    started = time.time()
    bytes_in = bytes_out = 0
    rooms_made = joins = resumes = dropped = 0


def new_code():
    while True:
        # Relay codes never start with 'P' (those belonged to direct-connection lobbies of the browser game).
        code = random.choice(ALPHABET.replace('P', '')) + ''.join(random.choice(ALPHABET) for _ in range(5))
        if code not in rooms:
            return code


def lobby_list():
    with rooms_lock:
        return [r.listing() for r in sorted(rooms.values(), key=lambda r: r.created)
                if r.public and r.players() <= MAX_GUESTS and r.host is not None]


def close_room(room, reason):
    """The host is gone for good: tell the guests (old clients know {"t":"rclose"}) and forget the room."""
    with rooms_lock:
        if rooms.get(room.code) is not room:
            return
        del rooms[room.code]
        guests = [g for g in room.guests.values() if g is not None]
    for g in guests:
        g.send({'t': 'rclose', 'reason': reason})
        g.room = g.role = None
    log.info('room %s closed (%s)', room.code, reason)


def drop_guest(room, slot, reason):
    """A guest is gone for good: free the slot and tell the host."""
    with rooms_lock:
        if slot not in room.guests:
            return
        del room.guests[slot]
        room.away.pop(slot, None)
        room.tokens.pop(slot, None)
        host = room.host
    if host is not None:
        host.send({'t': 'rleave', 'slot': slot, 'reason': reason})
    log.info('guest %s left room %s (%s)', slot, room.code, reason)


def public_link(handler=None):
    if Cfg.public_url:
        return Cfg.public_url
    url = ngrok_url()
    if url:
        return url
    if handler is not None:
        host = handler.headers.get('X-Forwarded-Host') or handler.headers.get('Host') or ''
        name = host.split(':')[0]
        if host and name not in ('localhost', '127.0.0.1', '0.0.0.0') and not name.startswith(('192.168.', '10.')):
            proto = handler.headers.get('X-Forwarded-Proto', 'http').split(',')[0].strip()
            return '%s://%s' % (proto, host)
    return None


_ngrok = {'url': None, 'at': 0.0}


def ngrok_url():
    """When ngrok runs on this PC, its https link (from ngrok's local API)."""
    if ON_CLOUD:
        return None
    if time.time() - _ngrok['at'] > 10:
        _ngrok['at'] = time.time()
        try:
            with urllib.request.urlopen('http://127.0.0.1:4040/api/tunnels', timeout=0.5) as r:
                tunnels = json.load(r).get('tunnels', [])
            https = [t['public_url'] for t in tunnels if t.get('public_url', '').startswith('https')]
            _ngrok['url'] = https[0] if https else None
        except (OSError, ValueError, KeyError):
            _ngrok['url'] = None
    return _ngrok['url']


def clean_text(s, n):
    s = ''.join(ch for ch in str(s or '') if ch.isprintable())
    return s.strip()[:n]


# ------------------------------------------------------------------------------------------------ relay session
def relay(handler, ws):
    """Runs on the connection's handler thread until it closes."""
    try:
        while True:
            msg = ws.recv()
            if msg is None:
                break
            if ws.room is None:
                lobby_message(handler, ws, msg)
                continue
            room, role = ws.room, ws.role
            if msg.startswith('{"t":"r') or msg.startswith('{"t": "r'):
                try:
                    d = json.loads(msg)
                except ValueError:
                    continue
                if d.get('t') in CONTROL:
                    room_control(ws, room, d)
                    continue
            if role == 'host':
                to = None
                if '"_to"' in msg:
                    try:
                        to = json.loads(msg).get('_to')
                    except ValueError:
                        continue
                with rooms_lock:
                    if to is None:
                        targets = [g for g in room.guests.values() if g is not None]
                    else:
                        g = room.guests.get(to)
                        targets = [g] if g is not None else []
                out = Outgoing(msg)
                for t in targets:
                    t.send(out)
                room.bytes += len(msg) * len(targets)
            else:
                # Guest -> host, tagged with the sender's slot.
                if '"_f"' in msg or not msg.startswith('{'):
                    try:
                        d = json.loads(msg)
                        if not isinstance(d, dict):
                            continue
                        d['_f'] = role
                        msg = json.dumps(d, separators=(',', ':'))
                    except ValueError:
                        continue
                else:
                    msg = '{"_f":%d,%s' % (role, msg[1:]) if msg != '{}' else '{"_f":%d}' % role
                host = room.host
                if host is not None:
                    host.send(Outgoing(msg))
                    room.bytes += len(msg)
    finally:
        ended(ws)


def lobby_message(handler, ws, msg):
    try:
        d = json.loads(msg)
    except ValueError:
        return
    if not isinstance(d, dict):
        return
    t = d.get('t')
    try:
        ws.v = max(1, int(d.get('v', 1)))
    except (TypeError, ValueError):
        ws.v = 1
    if t == 'rlist':
        ws.send({'t': 'rrooms', 'rooms': lobby_list()})
    elif t == 'rhost':
        if d.get('resume'):
            return resume_host(handler, ws, str(d.get('resume')).upper(), str(d.get('token', '')))
        with rooms_lock:
            err = None
            if len(rooms) >= Cfg.max_rooms:
                err = 'server-full'
            elif sum(1 for r in rooms.values() if r.host_ip == ws.ip) >= Cfg.max_rooms_per_ip:
                err = 'too-many-rooms'
            else:
                room = Room(new_code(), ws)
                rooms[room.code] = room
                ws.room, ws.role = room, 'host'
                Stats.rooms_made += 1
        if err:
            log.warning('room refused for %s: %s', ws.ip, err)
            ws.send({'t': 'rerr', 'error': err})
            return
        apply_meta(room, d)
        ws.send({'t': 'rcode', 'code': room.code, 'public': public_link(handler), 'token': room.host_token, 'v': 2})
        log.info('room %s created by %s (v%d%s)', room.code, ws.ip, ws.v, ', public' if room.public else '')
    elif t == 'rjoin':
        code = str(d.get('code', '')).upper().strip()
        token = str(d.get('token', ''))
        with rooms_lock:
            room = rooms.get(code)
            slot, resumed, err = None, False, None
            if room is None:
                err = 'peer-unavailable'
            else:
                if token:
                    slot = next((s for s, tk in room.tokens.items() if secrets.compare_digest(tk, token)), None)
                    resumed = slot is not None
                if slot is None:
                    free = [s for s in range(1, MAX_GUESTS + 1) if s not in room.guests]
                    if not free:   # a slot held for a dropped guest goes to a newcomer only when nothing else is free
                        free = sorted(room.away, key=lambda s: room.away[s])[:1]
                        for s in free:
                            room.guests.pop(s, None)
                            room.away.pop(s, None)
                            room.tokens.pop(s, None)
                            if room.host is not None:
                                room.host.send({'t': 'rleave', 'slot': s, 'reason': 'timeout'})
                    if not free:
                        err = 'full'
                    else:
                        slot = free[0]
            if not err:
                old = room.guests.get(slot)
                if old is not None and old is not ws:
                    old.replaced = True
                    old.kill()
                room.guests[slot] = ws
                room.away.pop(slot, None)
                if not resumed:
                    room.tokens[slot] = secrets.token_urlsafe(12)
                ws.room, ws.role = room, slot
                host = room.host
        if err:
            log.info('join failed for code %s from %s: %s', code, ws.ip, err)
            if err == 'peer-unavailable' and ips.bad_join(ws.ip):
                err = 'rate-limited'
            ws.send({'t': 'rerr', 'error': err})
            return
        reply = {'t': 'ropen', 'slot': slot, 'token': room.tokens[slot]}
        if resumed:
            reply['resumed'] = True
            Stats.resumes += 1
        ws.send(reply)
        if room.host_away_until and ws.v >= 2:
            ws.send({'t': 'rhostwait', 'secs': max(1, int(room.host_away_until - time.monotonic()))})
        if not resumed and host is not None:
            host.send({'t': 'rjoin', 'slot': slot})
        Stats.joins += 1
        log.info('guest %s %s room %s (%s, v%d)', slot, 'rejoined' if resumed else 'joined', code, ws.ip, ws.v)


def resume_host(handler, ws, code, token):
    with rooms_lock:
        room = rooms.get(code)
        if room is None or not token or not secrets.compare_digest(room.host_token, token):
            room = None
        else:
            old = room.host
            if old is not None and old is not ws:
                old.replaced = True
                old.kill()
            room.host = ws
            room.host_away_until = None
            room.host_v = max(room.host_v, ws.v)
            ws.room, ws.role = room, 'host'
            slots = sorted(s for s, g in room.guests.items() if g is not None or s in room.away)
            back = [g for g in room.guests.values() if g is not None and g.v >= 2]
    if room is None:
        log.info('host resume for %s failed (room gone)', code)
        ws.send({'t': 'rerr', 'error': 'room-gone'})
        return
    Stats.resumes += 1
    ws.send({'t': 'rcode', 'code': room.code, 'public': public_link(handler), 'token': room.host_token, 'v': 2, 'resumed': True})
    ws.send({'t': 'rpeers', 'slots': slots})
    for g in back:
        g.send({'t': 'rhostback'})
    log.info('host of room %s reconnected (%s)', code, ws.ip)


def apply_meta(room, d):
    if 'public' in d:
        room.public = bool(d.get('public'))
    if 'name' in d:
        room.name = clean_text(d.get('name'), 32)
    if 'mode' in d:
        room.mode = clean_text(d.get('mode'), 16)
    if 'state' in d:
        room.state = 'playing' if d.get('state') == 'playing' else 'lobby'


def room_control(ws, room, d):
    t = d.get('t')
    if t == 'rlist':
        ws.send({'t': 'rrooms', 'rooms': lobby_list()})
    elif t == 'rmeta' and ws.role == 'host':
        apply_meta(room, d)
    elif t == 'rkick' and ws.role == 'host':
        try:
            slot = int(d.get('slot'))
        except (TypeError, ValueError):
            return
        with rooms_lock:
            g = room.guests.get(slot)
        if g is not None:
            g.send({'t': 'rerr', 'error': 'kicked'})
            g.room = g.role = None
            g.close(1000, 'kicked')
        drop_guest(room, slot, 'kicked')


def ended(ws):
    """A connection is over: decide what its room hears. (The caller then closes it, which also stops its writer.)"""
    room, role = ws.room, ws.role
    ws.room = None
    if room is None or ws.replaced:
        return
    graceful = ws.got_close or SHUTTING_DOWN.is_set()
    if role == 'host':
        with rooms_lock:
            if room.host is not ws:
                return
            if graceful or room.host_v < 2 or Cfg.host_grace <= 0:
                hold = False
            else:
                hold = True
                room.host = None
                room.host_away_until = time.monotonic() + Cfg.host_grace
                waiting = [g for g in room.guests.values() if g is not None and g.v >= 2]
        if not hold:
            close_room(room, 'server-restart' if SHUTTING_DOWN.is_set() else 'host-left')
            return
        for g in waiting:
            g.send({'t': 'rhostwait', 'secs': Cfg.host_grace})
        log.info('host of room %s dropped - holding the room %ds for a reconnect', room.code, Cfg.host_grace)
    else:
        with rooms_lock:
            if room.guests.get(role) is not ws:
                return
            if graceful or ws.v < 2 or Cfg.guest_grace <= 0:
                hold = False
            else:
                hold = True
                room.guests[role] = None
                room.away[role] = time.monotonic() + Cfg.guest_grace
        if not hold:
            drop_guest(room, role, 'left')
        else:
            log.info('guest %s of room %s dropped - holding the slot %ds', role, room.code, Cfg.guest_grace)


def heartbeat():
    """Every few seconds: ping everyone, drop the silent, expire held rooms and slots, log a summary now and then.
    (One bad pass must never end this thread: without it nobody is pinged, silent links are never dropped and held
    rooms never expire - the "rooms 1, clients 0" stale room after a co-op test.)"""
    last_stats = time.monotonic()
    while not SHUTTING_DOWN.wait(min(5, Cfg.ping_interval)):
        try:
            last_stats = heartbeat_pass(last_stats)
        except Exception:
            log.exception('heartbeat pass failed (continuing)')


def heartbeat_pass(last_stats):
    if True:
        now = time.monotonic()
        for c in list(clients):
            if not c.alive:
                continue
            if now - c.last_rx > Cfg.ping_timeout:
                log.info('%s went silent for %ds - dropping it', c.label(), int(now - c.last_rx))
                c.kill()
            elif c.room is None and now - c.created > Cfg.idle_timeout:
                c.close(1000, 'idle')
            elif now - c.last_ping >= Cfg.ping_interval:
                c.last_ping = now
                c.ping()
        expired_rooms, expired_slots = [], []
        with rooms_lock:
            for r in list(rooms.values()):
                if r.host is None and r.host_away_until and now > r.host_away_until:
                    expired_rooms.append(r)
                for s, until in list(r.away.items()):
                    if now > until:
                        expired_slots.append((r, s))
        for r in expired_rooms:
            close_room(r, 'host-timeout')
        for r, s in expired_slots:
            drop_guest(r, s, 'timeout')
        ips.prune()
        if now - last_stats > 600:
            last_stats = now
            log.info('stats: %d rooms, %d connections, %.1f MB out / %.1f MB in since start',
                     len(rooms), len(clients), Stats.bytes_out / 1e6, Stats.bytes_in / 1e6)
    return last_stats


# ------------------------------------------------------------------------------------------------ remote version.json
_remote = {'body': None, 'at': 0.0, 'lock': threading.Lock()}


def remote_version():
    """version.json fetched from VERSION_URL (cached for a minute; the last good copy is kept if the fetch fails)."""
    with _remote['lock']:
        if _remote['body'] is None or time.time() - _remote['at'] > 60:
            try:
                req = urllib.request.Request(Cfg.version_url, headers={'User-Agent': 'VytelServer/' + SERVER_VERSION})
                with urllib.request.urlopen(req, timeout=8) as r:
                    body = r.read(65536)
                json.loads(body.decode('utf-8'))       # must be valid JSON
                _remote['body'] = body
                _remote['at'] = time.time()
            except (OSError, ValueError) as e:
                log.warning('could not fetch %s: %s', Cfg.version_url, e)
                _remote['at'] = time.time() - 45       # try again in ~15 s
        return _remote['body']


# ------------------------------------------------------------------------------------------------ HTTP
class Handler(SimpleHTTPRequestHandler):
    # HTTP/1.1 keep-alive (proxies reuse connections; WebSocket upgrades need an HTTP/1.1 "101").
    protocol_version = 'HTTP/1.1'
    timeout = 30
    server_version = 'VytelServer/' + SERVER_VERSION

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=Cfg.web_dir, **kwargs)

    # ---- helpers
    def client_ip(self):
        ip = self.client_address[0]
        trust = Cfg.trust_proxy
        if trust is None:   # behind a cloud proxy, or a tunnel (ngrok / cloudflared) running on this PC
            trust = ON_CLOUD or ip in ('127.0.0.1', '::1')
        if trust:
            fwd = self.headers.get('X-Forwarded-For', '')
            if fwd:
                return fwd.split(',')[0].strip() or ip
        return ip

    def end_headers(self):
        if not getattr(self, '_cache_set', False):
            self.send_header('Cache-Control', 'no-store')
        super().end_headers()

    def log_message(self, fmt, *args):
        path = getattr(self, 'path', '') or ''
        if path.startswith(('/ws', '/healthz', '/relay-info', '/rooms')):
            return
        try:
            ip = self.client_ip()
        except AttributeError:
            ip = self.client_address[0]
        log.info('%s %s', ip, fmt % args)

    def send_json(self, obj, status=200):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(body)

    def send_text(self, status, text):
        body = text.encode()
        self.send_response(status)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(body)

    # ---- routes
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.send_header('Access-Control-Allow-Private-Network', 'true')
        self.send_header('Content-Length', '0')
        self.end_headers()

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        self._cache_set = False
        path = self.path.split('?')[0]
        if path == '/ws':
            if 'websocket' in self.headers.get('Upgrade', '').lower():
                return self.handle_ws()
            return self.send_text(426, 'This is the game connection - open the game instead.')
        if path in ('/healthz', '/health'):
            return self.send_json({'ok': True, 'version': SERVER_VERSION, 'uptime': int(time.time() - Stats.started),
                                   'rooms': len(rooms), 'clients': len(clients), 'threads': threading.active_count(),
                                   'mb_out': round(Stats.bytes_out / 1e6, 1), 'mb_in': round(Stats.bytes_in / 1e6, 1)})
        if path == '/relay-info':
            return self.send_json({'relay': True, 'proto': 2, 'lobby': True, 'public': public_link(self), 'rooms': len(rooms)})
        if path == '/rooms':
            return self.send_json({'rooms': lobby_list()})
        if path == '/builds' or path.startswith('/builds/'):
            return self.serve_build(path[len('/builds/'):])
        if path == '/robots.txt':
            return self.send_text(200, 'User-agent: *\nDisallow: /builds/\n')
        if not Cfg.serve_web:
            if path == '/':
                return self.send_text(200, "Vytel's Academy Chronicles game server is running. Start the game with the launcher.")
            return self.send_text(404, 'Not found')
        return super().do_HEAD() if self.command == 'HEAD' else super().do_GET()

    def list_directory(self, path):
        self.send_error(404, 'Not found')
        return None

    # ---- launcher downloads: version.json + zips, with Range (resume), ETag and Content-Length
    def serve_build(self, name):
        name = urllib.request.unquote(name)
        if not name or '/' in name or '\\' in name or name.startswith('.'):
            return self.send_text(404, 'Not found')
        fpath = os.path.join(Cfg.builds_dir, name)
        if name == 'version.json' and not os.path.isfile(fpath) and Cfg.version_url:
            body = remote_version()
            if body is None:
                return self.send_text(502, 'Could not fetch version.json')
            self._cache_set = True
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            if self.command != 'HEAD':
                self.wfile.write(body)
            return
        if not os.path.isfile(fpath):
            return self.send_text(404, 'Not found')
        st = os.stat(fpath)
        size = st.st_size
        etag = '"%x-%x"' % (size, int(st.st_mtime))
        is_json = name.endswith('.json')
        ctype = 'application/json' if is_json else (mimetypes.guess_type(name)[0] or 'application/octet-stream')
        self._cache_set = True
        cache = 'no-cache' if is_json else 'public, max-age=3600'
        if self.headers.get('If-None-Match') == etag:
            self.send_response(304)
            self.send_header('ETag', etag)
            self.send_header('Cache-Control', cache)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        start, end, status = 0, size - 1, 200
        rng = self.headers.get('Range')
        if rng and (not self.headers.get('If-Range') or self.headers.get('If-Range') == etag):
            try:
                unit, spec = rng.split('=', 1)
                if unit.strip() != 'bytes' or ',' in spec:
                    raise ValueError
                a, b = spec.strip().split('-', 1)
                if a == '':
                    start, end = max(0, size - int(b)), size - 1
                else:
                    start, end = int(a), (min(int(b), size - 1) if b else size - 1)
                if start > end or start >= size:
                    self.send_response(416)
                    self.send_header('Content-Range', 'bytes */%d' % size)
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    return
                status = 206
            except ValueError:
                start, end, status = 0, size - 1, 200
        length = end - start + 1
        self.send_response(status)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(length))
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('ETag', etag)
        self.send_header('Last-Modified', formatdate(st.st_mtime, usegmt=True))
        self.send_header('Cache-Control', cache)
        self.send_header('Access-Control-Allow-Origin', '*')
        if status == 206:
            self.send_header('Content-Range', 'bytes %d-%d/%d' % (start, end, size))
        self.end_headers()
        if self.command == 'HEAD':
            return
        if status == 200 and not is_json:
            log.info('%s downloading %s (%.0f MB)', self.client_ip(), name, size / 1048576)
        self.wfile.flush()
        with open(fpath, 'rb') as f:
            try:
                self.connection.sendfile(f, start, length)
            except (OSError, ValueError):
                self.close_connection = True

    # ---- WebSocket upgrade
    def handle_ws(self):
        ip = self.client_ip()
        if SHUTTING_DOWN.is_set():
            return self.send_text(503, 'Server is restarting - try again in a minute.')
        if len(clients) >= Cfg.max_clients:
            log.warning('refusing %s: server full (%d connections)', ip, len(clients))
            return self.send_text(503, 'The game server is full right now - try again later.')
        why = ips.admit(ip)
        if why:
            log.warning('refusing %s: %s', ip, why)
            return self.send_text(429, why)
        try:
            key = self.headers.get('Sec-WebSocket-Key', '')
            if not key or self.headers.get('Sec-WebSocket-Version', '13') != '13':
                return self.send_text(400, 'Bad WebSocket request')
            accept = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
            ext = self.headers.get('Sec-WebSocket-Extensions', '') or ''
            deflate = Cfg.compress and 'permessage-deflate' in ext
            self.send_response_only(101, 'Switching Protocols')
            self.send_header('Upgrade', 'websocket')
            self.send_header('Connection', 'Upgrade')
            self.send_header('Sec-WebSocket-Accept', accept)
            self.send_header('X-Vytel-Relay', '2')
            if deflate:
                self.send_header('Sec-WebSocket-Extensions', 'permessage-deflate; server_no_context_takeover; client_no_context_takeover')
            self._cache_set = True
            super().end_headers()
            self.wfile.flush()
            self.close_connection = True
            try:
                self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
            # Anything the client already sent after the handshake is sitting in rfile's buffer: take it along.
            self.connection.settimeout(0.0)
            try:
                prefix = self.rfile.read1(65536) or b''
            except (OSError, ValueError):
                prefix = b''
            ws = Conn(self.connection, prefix, ip, deflate)
            clients.add(ws)
            try:
                relay(self, ws)
            finally:
                clients.discard(ws)
                ws.close()
        finally:
            ips.release(ip)


class Server(ThreadingHTTPServer):
    request_queue_size = 128      # a browser loading the web game asks for ~12 files at once
    daemon_threads = True
    # On Windows SO_REUSEADDR lets a SECOND server bind the same port (connections then land on either one at random).
    allow_reuse_address = sys.platform != 'win32'


SHUTTING_DOWN = threading.Event()


def shutdown(server):
    """Tell everyone the server is restarting, close the connections cleanly, stop."""
    if SHUTTING_DOWN.is_set():
        return
    SHUTTING_DOWN.set()
    log.info('shutting down: closing %d rooms, %d connections', len(rooms), len(clients))
    with rooms_lock:
        all_rooms = list(rooms.values())
    for r in all_rooms:
        close_room(r, 'server-restart')
    for c in list(clients):
        c.close(1001, 'server restarting')
    time.sleep(0.5)
    threading.Thread(target=server.shutdown, daemon=True).start()


def main(argv=None):
    load_config(argv)
    logging.basicConfig(level=getattr(logging, Cfg.log_level, logging.INFO), format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', stream=sys.stdout)
    try:
        server = Server((Cfg.host, Cfg.port), Handler)
    except OSError as e:
        print(f'Port {Cfg.port} is already in use ({e}) - another server.py (maybe an older one) is still running.')
        print('Close that window (or stop it with Ctrl+C) and run this again.', flush=True)
        sys.exit(1)
    threading.Thread(target=heartbeat, daemon=True, name='heartbeat').start()

    def on_signal(signum, frame):
        log.info('signal %s received', signum)
        threading.Thread(target=shutdown, args=(server,), daemon=True).start()

    for name in ('SIGTERM', 'SIGINT', 'SIGBREAK'):
        if hasattr(signal, name):
            try:
                signal.signal(getattr(signal, name), on_signal)
            except (ValueError, OSError):
                pass
    log.info("Vytel's Academy Chronicles server %s on http://%s:%d", SERVER_VERSION, Cfg.host, Cfg.port)
    log.info('relay: /ws   health: /healthz   lobby list: /rooms   launcher files: %s%s%s', Cfg.builds_dir,
             '   (version.json from %s)' % Cfg.version_url if Cfg.version_url else '', '   web game: on' if Cfg.serve_web else '')
    if Cfg.public_url:
        log.info('public address: %s', Cfg.public_url)
    elif not ON_CLOUD:
        log.info('share it online with a tunnel, e.g.:  ngrok http %d', Cfg.port)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        shutdown(server)
    finally:
        server.server_close()
        log.info('stopped')


if __name__ == '__main__':
    main()
