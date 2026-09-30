---
name: testing-moonlight-web
description: How to run moonlight-web locally and fake a Sunshine host (HTTP/HTTPS + data.json pair_info injection) to test host/pair/stream error paths end-to-end without real hardware
---

# Testing moonlight-web without a real Sunshine host

## Run the app
- For local testing, first create or update `server/config.json` with `web_server.bind_address` set to `127.0.0.1:8080` so the UI only listens on loopback (default `0.0.0.0` exposes the unauthenticated api — e.g. `POST /api/host` makes the relay send outbound requests — to the LAN). Don't pass `--bind-address` or set `BIND_ADDRESS`: those override the config value.
- `cargo run` in the repo root serves the UI on the bind address (serves `dist/`; build it once with `npm run build`).
- Config defaults to `./server/config.json` (auto-generated if missing); persisted state is `server/data.json` relative to CWD.

## Inject hosts directly into `server/data.json`
Stop the relay, write a V1-schema file (simplest input format; the relay migrates it to V4 on first store):
```json
{"hosts": [
  {"address": "127.0.0.1", "http_port": 47989,
   "cache": {"name": "FakeSunshine", "mac": "AA:BB:CC:DD:EE:FF"},
   "paired": {"client_private_key": "<PEM>", "client_certificate": "<PEM>", "server_certificate": "<PEM>"}},
  {"address": "127.0.0.1", "http_port": 9,
   "cache": {"name": "DeadHost", "mac": null}, "paired": null}
]}
```
- `paired: null` → unpaired host; a `paired` object with real PEMs → paired host. Generate PEMs with `openssl req -x509 -newkey rsa:2048 -nodes -subj /CN=x -keyout k.pem -out c.pem -days 3 -addext "basicConstraints=critical,CA:TRUE"`. PEM tags are not checked.
- A host entry with `paired` set makes the relay immediately do an HTTP serverinfo + HTTPS serverinfo + HTTPS applist exchange on every use; a dead address → `host_unreachable` everywhere — useful for failure-path testing.
- Undetailed host entries can't be added through the UI when the address is unreachable (`POST /api/host` verifies reachability), so injecting dead hosts via data.json is the only way to get them into the list.

## Fake Sunshine (python3)
A `ThreadingHTTPServer` is enough to fake the Moonlight HTTP API:
- Plain HTTP on the host's `http_port` serving `GET /serverinfo` (XML, `status_code="200"` attr on `<root>`). Required children: `hostname`, `appversion` (x.y.z.w), `GfeVersion`, `uniqueid` (UUID), `HttpsPort`, `MaxLumaPixelsHEVC`, `mac`, `LocalIP`, `ServerCodecModeSupport`, `PairStatus` (0/1), `currentgame`, `state` (must end with FREE or BUSY).
- HTTPS (self-signed cert that MUST equal the injected `pair_info.server_certificate` — the client pins it as the TLS root) serving `/serverinfo`, `/applist` (`<App><AppTitle>..</AppTitle><ID>n</ID></App>` children), `/launch`, `/pair`, `/cancel`.
- Return `<root status_code="500" status_message="...">` to trigger the `host_error` classification (`Sunshine reported an error: <msg>`; strings containing "video capture"/"encoding"/"display" get the friendlier "Sunshine failed to start encoding..." hint). A flag-file check inside the handler lets you flip an endpoint to failing mid-test (e.g. break `/applist` after page load to trigger refresh/app-list error notifications).
- Working example: `fake_sunshine.py` next to this guide (`python3 .agents/skills/testing-moonlight-web/fake_sunshine.py --http-port 47989 --https-port 47984 --cert cert.pem --key key.pem`; `--extra-http-port 48989` adds an unpaired-host listener, `--flags-dir` defaults to `/tmp/fakesun-flags`).

## Useful triggers
- Stream modal without clicking a game: open `/stream.html?hostId=<id>&appId=<n>` directly — same URL the game tile opens via `window.open`.
- Query params parsed by stream.html: `hostId`, `appId`, `dataTransport` (`auto|webrtc|websocket|webtransport`), `bitrate`, `fps`, `hdr`, `videoSize`, `videoSizeCustom.width/height`, `language`. Note it's `dataTransport`, NOT `transport`.
- With `dataTransport=webtransport` and WebTransport disabled in config, `apiWebTransportConfig` 404s, `tryWebTransportTransport()` returns `failednoconnect`, and the fatal modal shows "The relay isn't exposing or accepting your selected transport (WebTransport). Maybe it's disabled in the relay config?"
- Host tile click behavior: offline host (server_state null) → context menu (Send Wake Up Packet / Reload / Remove Host); online+paired → opens games; online+unpaired → starts pairing (PIN modal first, error message on failure).
- Notifications log via `console.error(message, errorObject)` (notification.ts) — error-level console entries mirroring shown notifications are intentional, not unhandled rejections. Failed HTTP fetches always log "Failed to load resource" in DevTools regardless of handling.

## Devin Secrets Needed
- none
