import argparse
import json
import socket
import sys
import time
import urllib.error
import urllib.request

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("base_url", help="DbGate base URL")
    parser.add_argument("--duration", type=float, default=None, help="optional maximum observation time in seconds")
    args = parser.parse_args()
    if args.duration is not None and args.duration <= 0:
        parser.error("--duration must be positive")

    base = args.base_url.rstrip("/")
    url = f"{base}/stream"
    print(f"[?] Requesting unauthenticated global SSE endpoint: {base}/stream")
    print(f"[?] Observation window: {args.duration:g}s" if args.duration is not None else "[?] Observation window: until interrupted")

    started = time.monotonic()
    events = 0
    try:
        request_timeout = args.duration + 2 if args.duration is not None else None
        with urllib.request.urlopen(url, timeout=request_timeout) as response:
            print(f"[*] HTTP {response.status} {response.headers.get('Content-Type', '')}".rstrip())
            if args.duration is not None:
                # Let a quiet SSE connection remain open until the actual
                # deadline; a short socket timeout would end the PoC early.
                response.fp.raw._sock.settimeout(args.duration)
            while args.duration is None or time.monotonic() - started < args.duration:
                try:
                    line = response.readline()
                except (socket.timeout, OSError):
                    break
                if not line:
                    break
                decoded = line.decode("utf-8", errors="replace").rstrip("\r\n")
                if decoded.startswith("data:"):
                    events += 1
                    payload = decoded[5:].strip()
                    try:
                        print("[*] EVENT", json.dumps(json.loads(payload), ensure_ascii=True, sort_keys=True))
                    except json.JSONDecodeError:
                        print("[*] EVENT", payload)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"[!] Connection failed: {exc}", file=sys.stderr)
        return 2

    except Exception as e:
        print(f"[!] Error: {e}", file=sys.stderr)
        return 2

    print(f"[*] Observed {events} event(s) before the stream closed or observation ended.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
