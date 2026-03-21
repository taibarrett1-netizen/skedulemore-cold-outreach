# Follow-up sends (`POST /api/follow-up/send`)

SkeduleMore **follow-ups** are triggered by the dashboard when a scheduled follow-up runs. The VPS only receives an HTTP request; it does **not** read follow-up configuration from `cold_dm_campaigns`, `cold_dm_message_group_messages`, or any follow-up-specific DB tables.

## Production contract (VPS)

- **Method / path:** `POST /api/follow-up/send` with `Content-Type: application/json` (and `Authorization: Bearer …` when `COLD_DM_API_KEY` is set).
- **Required:** `clientId`, `instagramSessionId`, `recipientUsername` (no `@` required; a leading `@` is stripped).
- **Voice follow-up:** `audioUrl` — **HTTPS** URL the worker **GET**s and saves to a temp file before the voice UI pipeline.
- **Optional with `audioUrl`:** `caption` — one text DM in the same thread **before** the voice note (`bot.js` → `sendPlainTextInThread` then voice).
- **Correlation (optional):** header **`X-Correlation-ID`** or **`X-Request-ID`**, or JSON **`correlationId`** / **`requestId`**. Logged on `[API] follow-up/send` and `[follow-up] …` lines.

**Strict modes:** exactly one of `text`, non-empty `messages[]`, or `audioUrl` (see table below). Implemented in `server.js` (`/api/follow-up/send`) and `sendFollowUp` in `bot.js`.

## Voice notes (follow-ups) — intended behaviour

Instagram **Web** does not expose a reliable “upload this `.wav` as a voice note” API for automation. The worker therefore uses this **single pipeline**:

1. **Download** `audioUrl` to a temp file (e.g. `/tmp/voice-note-….wav`).
2. **Play** that file into a **PulseAudio** virtual sink (`ffmpeg` → `VOICE_NOTE_SINK`, e.g. `ColdDMsVoice`) so Chromium’s default **microphone** capture hears your audio.
3. **Drive the normal IG voice UI**: focus composer → click/hold mic → “record” for the same duration as the file (+ small buffer) → click **Send**.

So logs will always show **both**:

- `Voice playback started (Xs): /tmp/voice-note-…` — ffmpeg feeding PulseAudio  
- `Voice (desktop): click mic, record X ms, then send` — Puppeteer using the mic UI  

That is **not** a bug or a double path (no separate “preview” vs “record”). The download + playback **is** how the audio reaches Instagram as a voice note.

If you pass **`caption`**, the worker sends **one text DM first** (`sendPlainTextInThread`), then the voice pipeline above (`bot.js`: `hasCaption` branch).

## Request body (strict modes)

Exactly **one** of:

| Field | Mode |
|--------|------|
| `text` | Single text DM |
| `messages` | Array of text DMs (sequential) |
| `audioUrl` | Voice follow-up (`caption` optional text before voice) |

## Debug screenshots (optional)

1. Set **`FOLLOW_UP_DEBUG_SCREENSHOTS=true`** in `.env` on the VPS and restart PM2.
2. For **voice** follow-ups, when debug screenshots are on you may get:
   - **`*_voice-mic-click-target.png`** — **before** the mic click, with a **red crosshair** at the exact viewport coordinates used (so you can see if we’re hitting the wrong icon).
   - **`*_voice-recording-ui-missed.png`** — if recording UI never appears after the click, same crosshair on the current page (usually wrong target or blocked mic).
   - **`*_voice-recording-ui-just-confirmed.png`** — **plain** screenshot ~220ms after the worker’s recording-UI check passes (blue bar / timer / dock heuristics). Use this to compare **what you see** vs what the heuristic matched.
   - **`*_voice-after-mic-click.png`** — ~600ms after recording starts; **red crosshair** still on the **mic** coordinates (reference only).
   - **`*_voice-recording-mid-hold.png`** — halfway through the ffmpeg/hold window (long clips only), **plain** — confirms the recording UI is still visible mid-capture.
   - **`*_voice-after-playback-before-send.png`** — **plain** screenshot after playback stops and **before** Send is clicked (composer / recording strip as IG shows it then).
   - **`*_voice-send-click-target.png`** — **red crosshair** on the **resolved Send** center (same logic as the click), with a short label naming the match reason (`dock_aria_send_generic`, `dock_rightmost_composer_band`, etc.).
   - **`*_voice-send-target-unresolved.png`** — if no Send control could be resolved for coordinates; crosshair is a **placeholder** — inspect `voice-after-playback-before-send` and PM2 logs (`dockedButtons=…`).
   Filenames include `correlationId` when sent in the request.
3. **Download via HTTP** (Bearer `COLD_DM_API_KEY` when set): `GET /api/debug/follow-up-screenshots` and `GET /api/debug/follow-up-screenshots/file?name=...`

Optional: **`FOLLOW_UP_SCREENSHOTS_FULL_PAGE=true`** for full-page PNGs.

### Watch the browser on the VPS (VNC + Xvfb)

See **`DEPLOYMENT.md`** → *Watching the browser on a VPS*. Set `HEADLESS_MODE=false`, `DISPLAY=:99` (or your Xvfb display), run `x11vnc`, tunnel with SSH, connect VNC to `localhost:5900`. Use **`PUPPETEER_SLOW_MO_MS=80`** so actions are easier to follow.

### Manual browser only (no send) — `POST /api/debug/follow-up/browser`

For VNC testing (e.g. voice mic, permissions) without sending a follow-up:

- **Path:** `POST /api/debug/follow-up/browser`  
- **Body (JSON):** `clientId`, `instagramSessionId`, optional `recipientUsername` (opens that DM thread if navigation succeeds).  
- **Auth:** Same as other `/api` routes — Bearer `COLD_DM_API_KEY` when set.  
- **Response:** **202** immediately; Chromium launches **in the background** on the server’s **`DISPLAY`** (must match Xvfb, e.g. `:98`).  
- **Behaviour:** Injects session cookies, opens Instagram, dismisses common home modals, optionally opens the DM — **no text/voice is sent.**  
- **Env:** **`HEADLESS_MODE=false`** and **`DISPLAY`** required to see the window. Optional **`FOLLOW_UP_DEBUG_BROWSER_MS`** (e.g. `1800000` = 30 min) to auto-close; if unset, the window stays until **PM2 restart**. Only **one** debug session at a time (409 if another is active).

### Recording UI gate (desktop)

Detection is **scoped to the composer dock** (the “Message…” row and strip just above it), so **blue outgoing bubbles** in the thread are **not** treated as recording UI.

The worker tries **several mic gestures in order**, and after **each** one polls until real recording UI appears or a per-attempt timeout hits:

1. `element.click()` on the mic node (with coordinate fallback)  
2. **`stepped_move+press_hold`** — short multi-step `mouse.move` toward the mic (small jitter), then `down` → hold **`VOICE_MIC_PRESS_HOLD_MS`** (default **210** ms, clamped 120–400) → `up` (often registers better than an instant click on desktop Web)  
3. `mouse` move → `down` → `up` at mic center (short hold)  
4. `mouse.click` at coordinates  
5. `elementFromPoint` + synthetic pointer/mouse events  

Composer-scoped detection uses the **same composer discovery** as focus/mic prep (`p[contenteditable]`, “add/write a message”, then first visible textbox) so logs are less likely to show **`lastWhy=no_composer`** when the dock is non-English or the placeholder omits the word “message”.

**ffmpeg → Pulse** starts only after that check passes: timer **`0:xx`/`1:xx`**, pause/delete **aria**, or a **thin blue strip** whose bottom edge sits **at the composer seam** (wide outgoing bubbles no longer count — they caused false “recording started”). The worker also requires **two matching detection polls** in a row so a one-frame glitch doesn’t start playback.

Mic gestures include a normal click, then **`mouse_hold_to_start_recording`** (long press, **`VOICE_MIC_START_HOLD_MS`** ~550 ms by default) for builds that behave more like “press to arm” in headless. If all attempts fail → **`voice_recording_ui_not_detected`**.

Optional env: **`VOICE_MIC_ATTEMPT_WAIT_MS`** (per gesture poll window); **`VOICE_RECORDING_UI_TIMEOUT_MS`** still influences the default when unset; **`VOICE_MIC_PRESS_HOLD_MS`** for the press-hold attempt.

### Stricter success criteria

By default **`VOICE_NOTE_STRICT_VERIFY`** is **on**: after clicking Send, the worker polls the thread until it sees a DOM change (e.g. new `audio` / list rows, scroll height in the message column, or new play/voice-related controls in the thread). If Instagram returns “success” in logs but no bubble appears, you should see a **`voice_not_confirmed_in_thread`** error instead of a false **`sent ok`**. Set **`VOICE_NOTE_STRICT_VERIFY=false`** only if this check causes false failures on your layout.

**If logs show `scroll=0` / `scrollerText=0` for the whole run:** the old heuristic often **could not find the message scroller**; the voice note may still have been sent. Check the thread in the app or in a VNC session. Recent worker builds add a **fallback scroller** (largest `overflow-y: auto|scroll` region in `main`) and **`mediaHints`** (play/voice/clip aria in the thread) so verification matches IG Web better.

### Reverse‑engineering IG Web (console, network)

Instagram’s minified JS rarely prints useful **`console.log`** for DMs. For your own debugging:

1. **Local Chrome (logged into the same account):** open the DM thread → **DevTools** → **Network** → filter **`graphql`** or **`ajax`** → record while you send a voice note manually. Inspect **request name / response** (often `.../graphql/query/` with doc IDs). That’s the real “API contract,” not the page console.
2. **Puppeteer:** after `const page = await browser.newPage()`, you can temporarily add  
   `page.on('console', (msg) => console.log('[browser]', msg.type(), msg.text()));`  
   and **`page.on('pageerror', …)`** to see page errors. That helps for **your** `evaluate()` scripts, not IG internals.
3. **CDP:** `const client = await page.target().createCDPSession(); await client.send('Log.enable'); client.on('Log.entryAdded', …)` for browser log entries (still sparse for IG).
4. **What to paste to an assistant:** a **HAR** export (sanitized), or **screenshots** of the Network row for the request fired when you tap Send on a voice note, plus **your PM2 log lines** (`Voice verify: …`, `mediaHints`).

**Note:** Sending **Escape** after recording was closing Instagram’s voice UI before “Send” — that is no longer done between record and send.

**Recording gesture:** On **desktop Chrome** the worker tries multiple activation methods until recording UI is confirmed, then holds for the audio duration (ffmpeg), then clicks **Send**. **ffmpeg is stopped before the Send click** so the virtual mic is not still streaming into a “recording” session. **Send** is resolved only inside the **composer dock** (and excludes Like/Heart/Gallery/Mic labels); the old “rightmost icon in the bottom strip” fallback could hit the **heart** and look like a success in logs while no voice note was sent.

**Mobile web** uses **press-and-hold** on the mic. The mic is resolved via layout (to the right of the message field, leftmost of the three trailing icons).

## Correlation (Supabase ↔ VPS logs)

Optional, for matching `execute_follow_up` / Edge logs to PM2:

- **Header:** `X-Correlation-ID` or `X-Request-ID`
- **JSON body:** `correlationId` or `requestId`

These are logged on `[API] follow-up/send` and `[follow-up] start` / `sent ok` / `failed` / `exception` lines as `correlationId=…`.

## Does `mode=voice` send scripted text?

**Not unless you include `caption`.** For `audioUrl` only (empty caption), `sendFollowUp` in `bot.js` does **not** call `sendPlainTextInThread`. It only navigates to the thread, then runs the voice pipeline.

If you see **duplicate opener-style text** in the thread:

- Check the dashboard isn’t **retrying** the same follow-up HTTP call (5xx retries).
- Check no **second** job (cold DM campaign, another follow-up) ran for the same user.
- This worker does not re-run “saved reply” or campaign templates on follow-up unless you sent `text` / `messages` / `caption`.

## What the debug screenshots showed (common issues)

- **Home (`01-home`):** If a **“Turn on Notifications”** modal is visible, it blocks the rest of the session until dismissed. The worker now clicks **Not Now** on that (and similar) modals **before** the `01-home` screenshot.
- **Composer (`04`):** A **sticker / GIF panel** over the thread steals clicks from the real mic/send. The worker now sends **Escape** several times before voice actions and **excludes emoji/sticker/GIF controls** when resolving the mic. The send step prefers **voice send** controls and avoids sticker regions.

### Clear debug PNGs on the VPS

From project root (where `follow-up-screenshots/` lives):

```bash
npm run clean-follow-up-screenshots
# or: rm -f follow-up-screenshots/*.png
```

### Per–mic-method screenshots

With **`FOLLOW_UP_DEBUG_SCREENSHOTS=true`**, each desktop mic attempt saves **`voice-mic-after_<method>.png`** with an on-image label:  
`METHOD: <name> | recordingUI=YES|no (why)`. Pick the method that first shows **`YES`**, then set **`VOICE_DESKTOP_MIC_METHOD=<exact name>`** so only that path runs.

Valid names (same order as default attempts):  
`element.click`, `mouse_hold_to_start_recording`, `stepped_move+press_hold`, `mouse_move+down+up`, `mouse.click_coords`, `elementFromPoint+pointer+mouse`.

### Send click nudge

After playback stops, the worker resolves the Send control, moves the mouse **slightly right** (default **14px**, **`VOICE_SEND_CLICK_NUDGE_X`**), then clicks with Puppeteer (falls back to in-page `el.click()` if needed).

### Recording UI not detected (but screenshots show the blue bar)

Headless Chromium sometimes **does not match** our DOM/`getComputedStyle` heuristics even when **`voice-recording-ui-missed.png`** shows an active recording strip (timing or paint differences).

- **`VOICE_ASSUME_RECORDING_AFTER_MIC=true`** — after the normal mic gesture sequence, still run **ffmpeg → Pulse**, hold for the audio duration, then **Send**, even if recording UI was never “confirmed.” Check screenshots / the thread; if recording never started, you may capture silence.
- **`VOICE_RECORDING_UI_CONFIRM_STREAK=1`** — require only **one** successful poll instead of two before treating recording UI as confirmed (default `2`).

## VPS requirements (voice)

- **`ffmpeg` and `ffprobe`** must be installed (`sudo apt install ffmpeg`). Without them the dashboard process can crash with `spawn ffmpeg ENOENT` when sending voice.
- **PulseAudio** null sink + `VOICE_NOTE_*` env (see `DEPLOYMENT.md`) for piping audio into the browser capture device.

## Logging

- **Dashboard (`server.js`):** Each request logs `[API] follow-up/send request …` and a line for `response ok=true/false` (see `pm2 logs ig-dm-dashboard` or `logs/bot.log` — same logger writes to stdout and `logs/bot.log`).
- **Send path (`bot.js`):** `[follow-up] start …` after session validation, `[follow-up] sent ok …` on success, and `[follow-up] failed …` / `[follow-up] exception …` on errors. Signed `audioUrl` values are not logged.

## Cold DM campaign voice (separate)

Voice for **cold outreach campaigns** may use `cold_dm_campaigns` / `cold_dm_message_group_messages` columns (see migration `010_voice_notes.sql`) and env `VOICE_NOTE_*` on the worker. That path is **independent** from follow-up sends.
