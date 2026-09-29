#!/usr/bin/env python3
"""Fake Sunshine host for testing moonlight-web error paths.

Implements just enough of the Moonlight HTTP API for the relay to treat this
as a real host:
  * plain HTTP  GET /serverinfo
  * HTTPS       GET /serverinfo /applist /launch /pair /cancel

Behavior toggles: files under --flags-dir flip endpoints mid-test without
restarting:
  fail_serverinfo  -> /serverinfo returns status_code 500
  fail_applist     -> /applist returns status_code 500
  fail_pair        -> /pair returns status_code 500 (default: it always does)
  ok_pair          -> /pair returns 200 (weak fake of the pairing exchange)
  ok_launch        -> /launch returns 200 (default: it 500s like a host that
                      can't initialize video capture/encoding)

Usage:
  python3 fake_sunshine.py --http-port 47989 --https-port 47984 \
      --cert cert.pem --key key.pem
The HTTPS cert MUST be the same PEM injected as the host's
pair_info.server_certificate in server/data.json (the relay pins it).
"""

import argparse
import ssl
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

FLAGS = Path("/tmp/fakesun-flags")


def flag(name: str) -> bool:
    return (FLAGS / name).exists()


def xml_ok(children: str) -> bytes:
    return f'<?xml version="1.0" encoding="utf-16"?><root status_code="200">{children}</root>'.encode()


def xml_err(message: str) -> bytes:
    return f'<?xml version="1.0" encoding="utf-16"?><root status_code="500" status_message="{message}"></root>'.encode()


def serverinfo(https_port: int, paired: bool) -> bytes:
    return xml_ok(
        "<hostname>FakeSunshine</hostname>"
        "<appversion>7.0.0.0</appversion>"
        "<GfeVersion>3.20.0.1</GfeVersion>"
        f"<uniqueid>{uuid.uuid4()}</uniqueid>"
        f"<HttpsPort>{https_port}</HttpsPort>"
        "<MaxLumaPixelsHEVC>1869449984</MaxLumaPixelsHEVC>"
        "<mac>AA:BB:CC:DD:EE:FF</mac>"
        "<LocalIP>127.0.0.1</LocalIP>"
        "<ServerCodecModeSupport>259</ServerCodecModeSupport>"
        f"<PairStatus>{1 if paired else 0}</PairStatus>"
        "<currentgame>0</currentgame>"
        "<state>SUNSHINE_DESKTOP_FREE</state>"
    )


def applist() -> bytes:
    return xml_ok(
        "<App><AppTitle>Desktop</AppTitle><ID>1</ID></App>"
        "<App><AppTitle>Steam</AppTitle><ID>2</ID></App>"
    )


def handle(path: str, https_port: int, paired: bool) -> bytes:
    if path.startswith("/serverinfo"):
        if flag("fail_serverinfo"):
            return xml_err("serverinfo broken for test")
        return serverinfo(https_port, paired)
    if path.startswith("/applist"):
        if flag("fail_applist"):
            return xml_err("applist broken for test")
        return applist()
    if path.startswith("/launch") or path.startswith("/resume"):
        if flag("ok_launch"):
            return xml_ok("<gamesession>1</gamesession>")
        return xml_err("Failed to initialize video capture/encoding. Is a display connected and turned on?")
    if path.startswith("/pair"):
        if flag("fail_pair") or not flag("ok_pair"):
            return xml_err("pairing disabled for test")
        # Only enough to satisfy a client polling pairstatus; the real
        # four-phase crypto exchange is out of scope for a fake.
        return xml_ok("<paired>1</paired>")
    if path.startswith("/cancel"):
        return xml_ok("<cancel>1</cancel>")
    if path.startswith("/unpair"):
        return xml_ok("")
    return xml_err("unknown endpoint")


def make_handler(https_port: int, paired: bool):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            try:
                body = handle(self.path.split("?")[0] + ("?" in self.path and "?" or ""), https_port, paired)
                self.send_response(200)
            except Exception as e:  # keep the fake alive on handler bugs
                body = xml_err(f"fake sunshine error: {e}")
                self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    return Handler


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--http-port", type=int, default=47989)
    p.add_argument("--https-port", type=int, default=47984)
    p.add_argument("--extra-http-port", type=int, default=0,
                   help="second plain-HTTP /serverinfo listener (e.g. 48989) that always reports PairStatus=0, for an unpaired host entry")
    p.add_argument("--cert", required=True, help="PEM cert for HTTPS; must equal pair_info.server_certificate injected into data.json")
    p.add_argument("--key", required=True)
    p.add_argument("--flags-dir", default="/tmp/fakesun-flags")
    args = p.parse_args()

    global FLAGS
    FLAGS = Path(args.flags_dir)
    FLAGS.mkdir(parents=True, exist_ok=True)

    httpd = ThreadingHTTPServer(("127.0.0.1", args.http_port), make_handler(args.https_port, True))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    print(f"[fakesun] HTTP  /serverinfo on 127.0.0.1:{args.http_port}")

    if args.extra_http_port:
        extra = ThreadingHTTPServer(("127.0.0.1", args.extra_http_port), make_handler(args.https_port, False))
        threading.Thread(target=extra.serve_forever, daemon=True).start()
        print(f"[fakesun] HTTP  /serverinfo (unpaired) on 127.0.0.1:{args.extra_http_port}")

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(args.cert, args.key)
    httpsd = ThreadingHTTPServer(("127.0.0.1", args.https_port), make_handler(args.https_port, True))
    httpsd.socket = ctx.wrap_socket(httpsd.socket, server_side=True)
    print(f"[fakesun] HTTPS on 127.0.0.1:{args.https_port}  flags: {FLAGS}")
    httpsd.serve_forever()


if __name__ == "__main__":
    main()
