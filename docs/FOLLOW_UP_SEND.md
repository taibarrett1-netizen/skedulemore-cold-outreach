# Follow-up sends (`POST /api/follow-up/send`)

SkeduleMore **follow-ups** are triggered by the dashboard when a scheduled follow-up runs. The VPS only receives an HTTP request; it does **not** read follow-up configuration from `cold_dm_campaigns`, `cold_dm_message_group_messages`, or any follow-up-specific DB tables.

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

## VPS requirements (voice)

- **`ffmpeg` and `ffprobe`** must be installed (`sudo apt install ffmpeg`). Without them the dashboard process can crash with `spawn ffmpeg ENOENT` when sending voice.
- **PulseAudio** null sink + `VOICE_NOTE_*` env (see `DEPLOYMENT.md`) for piping audio into the browser capture device.

## Logging

- **Dashboard (`server.js`):** Each request logs `[API] follow-up/send request …` and a line for `response ok=true/false` (see `pm2 logs ig-dm-dashboard` or `logs/bot.log` — same logger writes to stdout and `logs/bot.log`).
- **Send path (`bot.js`):** `[follow-up] start …` after session validation, `[follow-up] sent ok …` on success, and `[follow-up] failed …` / `[follow-up] exception …` on errors. Signed `audioUrl` values are not logged.

## Cold DM campaign voice (separate)

Voice for **cold outreach campaigns** may use `cold_dm_campaigns` / `cold_dm_message_group_messages` columns (see migration `010_voice_notes.sql`) and env `VOICE_NOTE_*` on the worker. That path is **independent** from follow-up sends.
