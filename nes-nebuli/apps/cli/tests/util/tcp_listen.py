#!/usr/bin/env python3
import argparse
import socket
import threading
from typing import Tuple


def handle_client(conn: socket.socket, addr: Tuple[str, int]) -> None:
    try:
        print(f"[+] Connected: {addr[0]}:{addr[1]}")
        while True:
            data = conn.recv(4096)
            if not data:
                break
            # Print raw bytes safely; use replace to avoid UnicodeDecodeError
            text = data.decode("utf-8", errors="replace")
            print(f"[{addr[0]}:{addr[1]}] {text}", end="" if text.endswith("\n") else "\n")
    except Exception as e:
        print(f"[!] Error with {addr[0]}:{addr[1]}: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        print(f"[-] Disconnected: {addr[0]}:{addr[1]}")


def serve(host: str, port: int) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((host, port))
        srv.listen()
        print(f"[*] Listening on {host}:{port}")

        while True:
            conn, addr = srv.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()


def main() -> None:
    p = argparse.ArgumentParser(description="Simple TCP server that prints all received data.")
    p.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    p.add_argument("--port", type=int, default=9000, help="Bind port (default: 9000)")
    args = p.parse_args()
    serve(args.host, args.port)


if __name__ == "__main__":
    main()