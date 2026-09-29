# ============================================================
#  client.py
#  Tugas Besar Jaringan Komputer - Client
#
#  Mode:
#   --mode tcp    -> HTTP GET via Proxy (uji caching, 404, dsb)
#   --mode udp    -> QoS test (RTT, jitter, packet loss, throughput)
#                    -> hasil dicetak di terminal DAN disimpan ke CSV
#   --mode multi  -> simulasi beberapa client TCP secara bersamaan
#                    (uji multithreading proxy/server, no crash/deadlock)
# ============================================================

import socket
import time
import argparse
import datetime
import statistics
import csv
import threading

# ── KONFIGURASI ────────────────────────────────────────────
PROXY_HOST  = "10.241.154.18"   
PROXY_PORT  = 8080

SERVER_HOST = "10.241.154.18"   
UDP_PORT    = 9000

BUFFER_SIZE = 4096
print_lock  = threading.Lock()


def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [CLIENT] {msg}")


# ============================================================
#  MODE TCP : HTTP GET via Proxy
# ============================================================
def http_get(path, host=PROXY_HOST, port=PROXY_PORT, timeout=10):
    """Mengirim HTTP GET dan mengembalikan (status_line, header, body, elapsed_ms)."""
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode()

    start = time.time()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect((host, port))
    sock.sendall(request)

    response = b""
    while True:
        chunk = sock.recv(BUFFER_SIZE)
        if not chunk:
            break
        response += chunk
    sock.close()
    elapsed = (time.time() - start) * 1000

    if b"\r\n\r\n" in response:
        header_part, body_part = response.split(b"\r\n\r\n", 1)
    else:
        header_part, body_part = response, b""

    status_line = header_part.split(b"\r\n")[0].decode(errors="replace")
    return status_line, header_part.decode(errors="replace"), body_part, elapsed


def mode_tcp(path):
    log(f"Mode TCP | GET {path} via Proxy {PROXY_HOST}:{PROXY_PORT}")
    try:
        status_line, header, body, elapsed = http_get(path)
        print("\n" + "=" * 55)
        print(f"STATUS   : {status_line}")
        print(f"WAKTU    : {elapsed:.2f} ms")
        print("-" * 55)
        print("HEADER:")
        print(header)
        print("-" * 55)
        print("BODY (preview 1000 karakter):")
        print(body.decode(errors="replace")[:1000])
        print("=" * 55)
    except ConnectionRefusedError:
        log("ERROR: Koneksi ke proxy ditolak. Pastikan proxy.py berjalan.")
    except socket.timeout:
        log("TIMEOUT: Proxy tidak merespons.")
    except Exception as e:
        log(f"ERROR: {e}")


# ============================================================
#  MODE UDP : QoS Test -> CSV + ringkasan di terminal
# ============================================================
def mode_udp(count=10):
    log(f"Mode UDP QoS | Mengirim {count} paket ke {SERVER_HOST}:{UDP_PORT}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(1)  # timeout 1 detik per paket

    results = []   # list of dict: seq, rtt_ms, status
    rtt_list = []
    received_bytes = 0

    test_start = time.time()
    print("\n" + "=" * 55)
    for seq in range(1, count + 1):
        send_time = time.time()
        payload = f"Ping {seq} {send_time}".encode()

        try:
            sock.sendto(payload, (SERVER_HOST, UDP_PORT))
            data, _ = sock.recvfrom(BUFFER_SIZE)
            recv_time = time.time()
            rtt = (recv_time - send_time) * 1000

            rtt_list.append(rtt)
            received_bytes += len(data)
            results.append({"seq": seq, "rtt_ms": f"{rtt:.3f}", "status": "OK"})
            print(f"  Paket {seq:>3}: RTT = {rtt:.3f} ms")

        except socket.timeout:
            results.append({"seq": seq, "rtt_ms": "", "status": "TIMEOUT"})
            print(f"  Paket {seq:>3}: Request timed out")

        time.sleep(0.2)

    test_duration = time.time() - test_start
    sock.close()

    # ── HITUNG STATISTIK ────────────────────────────────────
    sent = count
    received = len(rtt_list)
    lost = sent - received
    loss_pct = (lost / sent) * 100 if sent else 0

    rtt_min = min(rtt_list) if rtt_list else 0
    rtt_avg = sum(rtt_list) / len(rtt_list) if rtt_list else 0
    rtt_max = max(rtt_list) if rtt_list else 0

    if len(rtt_list) >= 2:
        deltas = [abs(rtt_list[i] - rtt_list[i - 1]) for i in range(1, len(rtt_list))]
        jitter = statistics.stdev(deltas) if len(deltas) > 1 else deltas[0]
    else:
        jitter = 0

    # Throughput (kbps) = total bit diterima / durasi total
    throughput_kbps = (received_bytes * 8 / test_duration) / 1000 if test_duration > 0 else 0

    # ── CETAK RINGKASAN ──────────────────────────────────────
    print("-" * 55)
    print(f"Statistik QoS ({SERVER_HOST}:{UDP_PORT})")
    print(f"  Paket dikirim  : {sent}")
    print(f"  Paket diterima : {received}")
    print(f"  Paket hilang   : {lost}")
    print(f"  Packet Loss    : {loss_pct:.2f} %")
    print(f"  RTT Min        : {rtt_min:.3f} ms")
    print(f"  RTT Avg        : {rtt_avg:.3f} ms")
    print(f"  RTT Max        : {rtt_max:.3f} ms")
    print(f"  Jitter         : {jitter:.3f} ms")
    print(f"  Throughput     : {throughput_kbps:.2f} kbps")
    print("=" * 55)

    # ── SIMPAN KE CSV ─────────────────────────────────────────
    timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"qos_result_{timestamp_str}.csv"

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        # Detail per paket
        writer.writerow(["seq", "rtt_ms", "status"])
        for r in results:
            writer.writerow([r["seq"], r["rtt_ms"], r["status"]])

        # Baris kosong + ringkasan
        writer.writerow([])
        writer.writerow(["Parameter", "Value"])
        writer.writerow(["Packets Sent", sent])
        writer.writerow(["Packets Received", received])
        writer.writerow(["Packet Loss (%)", f"{loss_pct:.2f}"])
        writer.writerow(["RTT Min (ms)", f"{rtt_min:.3f}"])
        writer.writerow(["RTT Avg (ms)", f"{rtt_avg:.3f}"])
        writer.writerow(["RTT Max (ms)", f"{rtt_max:.3f}"])
        writer.writerow(["Jitter (ms)", f"{jitter:.3f}"])
        writer.writerow(["Throughput (kbps)", f"{throughput_kbps:.2f}"])

    log(f"Hasil QoS disimpan ke file: {filename}")


# ============================================================
#  MODE MULTI : Simulasi beberapa client TCP bersamaan
# ============================================================
def worker_request(client_id, path):
    try:
        status_line, _, body, elapsed = http_get(path)
        with print_lock:
            print(f"[Client-{client_id}] {status_line} | {elapsed:.2f} ms | path={path} | size={len(body)} bytes")
    except Exception as e:
        with print_lock:
            print(f"[Client-{client_id}] ERROR: {e}")


def mode_multi(num_clients, path):
    log(f"Mode MULTI-CLIENT | {num_clients} client mengakses {path} secara bersamaan via Proxy")

    threads = []
    start = time.time()
    for i in range(1, num_clients + 1):
        t = threading.Thread(target=worker_request, args=(i, path))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    elapsed = (time.time() - start) * 1000
    log(f"Selesai. Total waktu untuk {num_clients} client: {elapsed:.2f} ms")
    log("Jika semua baris di atas muncul tanpa error/hang, server/proxy aman dari deadlock.")


# ============================================================
#  MAIN
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client Jaringan Komputer - Tugas Besar")
    parser.add_argument("--mode", choices=["tcp", "udp", "multi"], required=True,
                         help="tcp = HTTP via proxy | udp = QoS test | multi = simulasi multi-client")
    parser.add_argument("--path", default="/index.html",
                         help="Path resource (default: /index.html)")
    parser.add_argument("--count", type=int, default=10,
                         help="Jumlah paket UDP (default: 10)")
    parser.add_argument("--clients", type=int, default=5,
                         help="Jumlah client untuk mode multi (default: 5)")
    args = parser.parse_args()

    log(f"Client dimulai | mode: {args.mode.upper()}")

    if args.mode == "tcp":
        mode_tcp(args.path)
    elif args.mode == "udp":
        mode_udp(args.count)
    elif args.mode == "multi":
        mode_multi(args.clients, args.path)
