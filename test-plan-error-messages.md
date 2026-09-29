# Test Plan: Client-facing error messages (PR #9, commit af9356d)

## Context
The relay now returns JSON `{code, message}` on every non-2xx API response, sends a
clientbound `{Error: {code, message}}` frame on WS/WebTransport before closing a failed
stream, and the frontend surfaces reasons via `showMessage`/`showNotification` and the
connect modal instead of silent throws / "Tried all transport options".

## Test environment (setup, done before recording)
- `cargo run` in `/home/ubuntu/repos/moonlight-web-stream-simplified` → UI on http://localhost:8080 (serves `dist/`).
- `server/data.json` injected (V1 schema → auto-migrated) with hosts:
  - `FakeSunshine` → 127.0.0.1:47989, `pair_info` = real openssl certs (paired, online while fake server runs)
  - `FakeUnpaired` → 127.0.0.1:48989, `pair_info` = null (online, NotPaired)
  - `DeadPaired` → 127.0.0.1:9 (closed port), `pair_info` = certs (paired, offline)
- Fake Sunshine (python3): HTTP :47989 (serverinfo PairStatus=1), HTTP :48989 (serverinfo PairStatus=0),
  HTTPS :47984 (self-signed cert matching injected `server_certificate`) serving
  `/serverinfo` (200), `/applist` (200, app id=1 "Fake Desktop"; flips to status_code=500
  `status_message="applist broken for test"` when `/tmp/fail_applist` exists),
  `/launch` → status_code=500 `status_message="Failed to initialize video capture/encoding. Is a display connected and turned on?"`,
  `/pair` → status_code=500 `status_message="pairing disabled for test"`.
- Chrome maximized at http://localhost:8080, recording on.

Why this distinguishes broken vs working: before the change these paths threw
unhandled rejections / showed only "Tried all transport options"; the assertions below
require the new relay-provided strings to be visible on screen.

## Tests

### T1 — Sanity: UI loads, injected hosts render (Regression)
- Open http://localhost:8080.
- PASS: page renders; tiles for `FakeSunshine` (paired/online), `FakeUnpaired` (unpaired), `DeadPaired` (offline) visible.

### T2 — Add host: unreachable address
- Click "Add Host"/+ button → address `127.0.0.1`, port `9` → submit.
- PASS: notification appears containing `Host "127.0.0.1" is not reachable` (i18n `addHostUnreachable`; relay returns 404 host_not_found). FAIL if silent or generic.

### T3 — Add host: reachable but not Sunshine (JSON apiError path)
- Add host → address `127.0.0.1`, port `8080` (the relay itself answers HTTP).
- PASS: notification shows relay-provided reason, e.g. `Couldn't talk to the host: ...` — must NOT be the "is not reachable" text and must not be silent. (host_add succeeds TCP connect but response isn't valid Moonlight XML → 502 apiError.message shown.)

### T4 — Stream-start failure: dead paired host → WS Error frame in modal
- New tab: http://localhost:8080/stream.html?hostId=<DeadPaired id>&appId=1
- PASS: connect modal headline `Failed to connect to the host` AND description containing
  `Couldn't reach Sunshine. Make sure the host is on and Sunshine is running.`
  ("Show logs" panel shows per-transport debug lines.) FAIL if only "Tried all transport options" or bare close.

### T5 — Stream-start failure: Sunshine /launch error surfaces verbatim hint
- Main tab: click `FakeSunshine` tile → app list loads → `Fake Desktop` tile visible
  (proves paired HTTPS applist works).
- Click `Fake Desktop` → stream.html opens → WebRTC then WS attempts.
- PASS: modal headline `Failed to connect to the host` + description
  `Sunshine failed to start encoding. Make sure the host's display is connected and on.`
  (from Error frame built from /launch status_message via sunshine_error_hint).

### T6 — Host refresh + app-list failure notifications
- `touch /tmp/fail_applist` → back on host list → click `FakeSunshine` tile again.
- PASS: notifications `Couldn't refresh the host: Sunshine reported an error: applist broken for test`
  and/or `Couldn't load the app list: Sunshine reported an error: applist broken for test`.
  FAIL if silent or unhandled rejection only.

### T7 — Pairing failure message
- Click `FakeUnpaired` tile (online + NotPaired → click triggers pair()).
- PIN prompt may flash; PASS: modal `Couldn't pair with the host:` followed by a reason
  (`Sunshine reported an error: pairing disabled for test` or `Pairing with the host failed: ...`).
  FAIL if silent or unhandled rejection.

### T8 — Wake-up packet error (bonus)
- Context menu on `DeadPaired` → Send Wake Up packet (cache has no mac → HostNotFound).
- PASS: modal `Couldn't send the wake-up packet: ...` OR success toast — record actual.

### T9 — Console health
- browser_console after all tests.
- PASS: no unhandled promise rejections / no unexpected console.error for the exercised paths.

## Boundaries (report as untested if not reached)
- Remove-host failure (DELETE always succeeds server-side for existing ids).
- Cancel-session failure (needs a live current_game session).
- WebTransport-specific failure (webtransport disabled by default; may add `&transport=webtransport` bonus check).
