# ============================================================
#  webserver.py
#  Tugas Besar Jaringan Komputer - Web Server
#
#  - TCP HTTP Server (port 8000) : serve file statis (html, css, js, gambar)
#    -> 200 OK / 404 Not Found / 500 Internal Server Error
#  - UDP Echo Server (port 9000) : untuk pengukuran QoS (RTT/jitter/loss)
#  - Multithreading (thread-per-connection)
#  - Logging: IP client, path, timestamp, status code
# ============================================================

from socket import *
import threading
import os
import datetime
import sys

# ── KONFIGURASI ──────────────────────────────────────────────
SERVER_HOST = "0.0.0.0"
TCP_PORT    = 8000
UDP_PORT    = 9000
BUFFER_SIZE = 4096
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))


def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [SERVER] {msg}")


def get_content_type(filename):
    ext = os.path.splitext(filename)[1].lower()
    return {
        ".html": "text/html",
        ".htm":  "text/html",
        ".css":  "text/css",
        ".js":   "application/javascript",
        ".png":  "image/png",
        ".jpg":  "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif":  "image/gif",
        ".ico":  "image/x-icon",
        ".svg":  "image/svg+xml",
    }.get(ext, "application/octet-stream")


def handle_client(connectionSocket, addr):
    log(f"Koneksi masuk dari {addr} | Thread: {threading.current_thread().name}")
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

        message = raw.decode(errors="replace")
        request_line = message.split("\r\n")[0]
        parts = request_line.split()

        if len(parts) < 2:
            connectionSocket.sendall(
                b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n"
                b"<h1>400 Bad Request</h1>"
            )
            return

        method, path = parts[0], parts[1]

        if method != "GET":
            connectionSocket.sendall(
                b"HTTP/1.1 405 Method Not Allowed\r\nConnection: close\r\n\r\n"
                b"<h1>405 Method Not Allowed</h1>"
            )
            return

        if path == "/" or path == "":
            path = "/index.html"

        
        path = path.split("?")[0]

        
        safe_path = os.path.normpath(os.path.join(BASE_DIR, path.lstrip("/")))
        if not safe_path.startswith(BASE_DIR):
            connectionSocket.sendall(
                b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n"
                b"<h1>403 Forbidden</h1>"
            )
            return

        try:
            with open(safe_path, "rb") as f:
                outputdata = f.read()

            content_type = get_content_type(safe_path)
            header = (
                f"HTTP/1.1 200 OK\r\n"
                f"Content-Type: {content_type}; charset=utf-8\r\n"
                f"Content-Length: {len(outputdata)}\r\n"
                f"Connection: close\r\n"
                f"\r\n"
            )
            connectionSocket.sendall(header.encode() + outputdata)
            log(f"200 OK  | {addr[0]} | {path} | {len(outputdata)} bytes")

        except IOError:
            body = b"<html><body><h1>404 Not Found</h1><p>File tidak ditemukan di server.</p></body></html>"
            header = (
                f"HTTP/1.1 404 Not Found\r\n"
                f"Content-Type: text/html; charset=utf-8\r\n"
                f"Content-Length: {len(body)}\r\n"
                f"Connection: close\r\n"
                f"\r\n"
            )
            connectionSocket.sendall(header.encode() + body)
            log(f"404 NOT FOUND | {addr[0]} | {path}")

    except Exception as e:
        log(f"500 ERROR | {addr} | {e}")
        try:
            body = b"<html><body><h1>500 Internal Server Error</h1></body></html>"
            header = (
                f"HTTP/1.1 500 Internal Server Error\r\n"
                f"Content-Type: text/html; charset=utf-8\r\n"
                f"Content-Length: {len(body)}\r\n"
                f"Connection: close\r\n\r\n"
            )
            connectionSocket.sendall(header.encode() + body)
        except:
            pass
    finally:
        connectionSocket.close()


def start_tcp_server():
    serverSocket = socket(AF_INET, SOCK_STREAM)
    serverSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    serverSocket.bind((SERVER_HOST, TCP_PORT))
    serverSocket.listen(20)
    log(f"TCP HTTP Server siap di port {TCP_PORT} | Base dir: {BASE_DIR}")

    while True:
        connectionSocket, addr = serverSocket.accept()
        t = threading.Thread(target=handle_client, args=(connectionSocket, addr), daemon=True)
        t.start()
        log(f"Thread dibuat: {t.name} | Aktif: {threading.active_count() - 1}")


def start_udp_server():
    udpSocket = socket(AF_INET, SOCK_DGRAM)
    udpSocket.bind((SERVER_HOST, UDP_PORT))
    log(f"UDP Echo Server siap di port {UDP_PORT}")

    while True:
        try:
            data, clientAddr = udpSocket.recvfrom(BUFFER_SIZE)
            udpSocket.sendto(data, clientAddr)  
            log(f"UDP echo | {clientAddr[0]}:{clientAddr[1]} | {data.decode(errors='replace')}")
        except Exception as e:
            log(f"UDP ERROR: {e}")


if __name__ == "__main__":
    log("=" * 55)
    log("  Web Server - Tugas Besar Jaringan Komputer")
    log(f"  TCP (HTTP) : port {TCP_PORT}")
    log(f"  UDP (QoS)  : port {UDP_PORT}")
    log(f"  Base dir   : {BASE_DIR}")
    log("=" * 55)

    udp_thread = threading.Thread(target=start_udp_server, daemon=True)
    udp_thread.start()

    try:
        start_tcp_server()
    except KeyboardInterrupt:
        log("Server dihentikan.")
        sys.exit()
