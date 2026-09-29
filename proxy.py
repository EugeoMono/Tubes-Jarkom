# ============================================================
#  proxy.py
#  Tugas Besar Jaringan Komputer - Proxy Server
#
#  - TCP Proxy (port 8080)
#  - Forwarding request ke Web Server
#  - Caching sederhana: simpan response ke file lokal
#       -> Cache HIT  : response diambil dari cache (cepat)
#       -> Cache MISS : forward ke server, simpan ke cache
#  - Error handling: 502 Bad Gateway / 504 Gateway Timeout
#  - Multithreading (thread-per-connection)
#  - Logging: IP client, URL, status cache (HIT/MISS), waktu respons
# ============================================================

from socket import *
import threading
import os
import datetime
import time

# ── KONFIGURASI ──────────────────────────────────────────────
PROXY_HOST  = "0.0.0.0"
PROXY_PORT  = 8080

SERVER_HOST = "10.241.154.18" 
SERVER_PORT = 8000

BUFFER_SIZE = 4096
CACHE_DIR   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
os.makedirs(CACHE_DIR, exist_ok=True)


def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [PROXY] {msg}")


def url_to_cache_path(url_path):
    safe = url_path.strip("/").replace("/", "_") or "index.html"
    return os.path.join(CACHE_DIR, safe)


def handle_client(connectionSocket, addr):
    start_time = time.time()
    try:
        raw = b""
        connectionSocket.settimeout(5)
        while b"\r\n\r\n" not in raw:
            chunk = connectionSocket.recv(BUFFER_SIZE)
            if not chunk:
                break
            raw += chunk

        if not raw:
            connectionSocket.close()
            return

        request_line = raw.split(b"\r\n")[0].decode(errors="replace")
        parts = request_line.split()
        if len(parts) < 2:
            connectionSocket.sendall(b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n")
            return

        url_path = parts[1].split("?")[0]
        if url_path in ("/", ""):
            url_path = "/index.html"

        cache_file = url_to_cache_path(url_path)

        # ── CACHE HIT ──────────────────────────────────────
        if os.path.isfile(cache_file):
            with open(cache_file, "rb") as f:
                response = f.read()
            connectionSocket.sendall(response)
            elapsed = (time.time() - start_time) * 1000
            log(f"CACHE HIT  | {addr[0]} | {url_path} | {elapsed:.2f} ms")
            connectionSocket.close()
            return

        # ── CACHE MISS -> FORWARD KE WEB SERVER ─────────────
        log(f"CACHE MISS | {addr[0]} | {url_path} | forwarding to {SERVER_HOST}:{SERVER_PORT}")

        forward_request = (
            f"GET {url_path} HTTP/1.1\r\n"
            f"Host: {SERVER_HOST}:{SERVER_PORT}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode()

        try:
            srv_sock = socket(AF_INET, SOCK_STREAM)
            srv_sock.settimeout(5)
            srv_sock.connect((SERVER_HOST, SERVER_PORT))
            srv_sock.sendall(forward_request)

            response = b""
            while True:
                chunk = srv_sock.recv(BUFFER_SIZE)
                if not chunk:
                    break
                response += chunk
            srv_sock.close()

        except timeout:
            log(f"504 GATEWAY TIMEOUT | {addr[0]} | {url_path}")
            connectionSocket.sendall(
                b"HTTP/1.1 504 Gateway Timeout\r\nConnection: close\r\n\r\n"
                b"<h1>504 Gateway Timeout</h1>"
            )
            return
        except (ConnectionRefusedError, OSError) as e:
            log(f"502 BAD GATEWAY | {addr[0]} | {url_path} | {e}")
            connectionSocket.sendall(
                b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\n\r\n"
                b"<h1>502 Bad Gateway</h1>"
            )
            return

        # Simpan ke cache HANYA jika response 200 OK
        if response.startswith(b"HTTP/1.1 200"):
            with open(cache_file, "wb") as f:
                f.write(response)

        connectionSocket.sendall(response)
        elapsed = (time.time() - start_time) * 1000
        status_line = response.split(b"\r\n")[0].decode(errors="replace")
        log(f"FORWARDED  | {addr[0]} | {url_path} | {status_line} | {elapsed:.2f} ms")

    except Exception as e:
        log(f"ERROR | {addr} | {e}")
        try:
            connectionSocket.sendall(b"HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\n\r\n")
        except:
            pass
    finally:
        connectionSocket.close()


if __name__ == "__main__":
    serverSocket = socket(AF_INET, SOCK_STREAM)
    serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    serverSocket.bind((PROXY_HOST, PROXY_PORT))
    serverSocket.listen(20)

    log("=" * 55)
    log("  Proxy Server - Tugas Besar Jaringan Komputer")
    log(f"  Listen      : port {PROXY_PORT}")
    log(f"  Forward ke  : {SERVER_HOST}:{SERVER_PORT}")
    log(f"  Cache dir   : {CACHE_DIR}")
    log("=" * 55)

    while True:
        connectionSocket, addr = serverSocket.accept()
        t = threading.Thread(target=handle_client, args=(connectionSocket, addr), daemon=True)
        t.start()
        log(f"Thread dibuat: {t.name} | Aktif: {threading.active_count() - 1}")
