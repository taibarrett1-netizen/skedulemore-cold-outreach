# VNC + Xvfb + manual Instagram browser (step-by-step)

Use this when you want to **see** Chromium on the VPS (TigerVNC on your Mac) and use **`POST /api/debug/follow-up/browser`** to open Instagram with a saved session — **without sending DMs**.

**Why people see a black screen:** VNC only shows what is drawn on the X display. If **no** app is running on that display (or Chromium never started / already exited), the screen stays black. This guide makes you prove **`xterm`** works **before** you rely on the bot.

**Pick one display number and use it everywhere.** This doc uses **`:98`** (change all `:98` if you prefer `:99`).

---

## Part A — Clean slate (VPS, SSH)

Do this over **SSH** as root (or your deploy user).

### A1. Stop VNC and optional test windows

```bash
pkill x11vnc 2>/dev/null || true
# Optional: stop stray xterms you started earlier (careful if you use xterm for other things)
pkill -f 'xterm.*DISPLAY=:98' 2>/dev/null || true
```

### A2. Reset the bot’s “debug browser already running” lock

The API allows **one** debug browser at a time. If a previous run crashed, Node can still think a session is active.

```bash
cd /path/to/your/ColdDMs/repo   
pm2 restart all                 # or: pm2 restart <your-app-name>
```

Wait until `pm2 status` shows **online**.

### A3. Install packages (once per server)

```bash
sudo apt update
sudo apt install -y xvfb x11vnc xterm fluxbox
```

---

## Part B — Virtual display (`Xvfb`) on `:98`

### B1. See if something already owns `:98`

```bash
ps aux | grep '[X]vfb'
```

If you see `Xvfb :98` already and you did **not** start it yourself, either reuse it or pick another display (e.g. `:99`) and use that **everywhere** below.

### B2. Start `Xvfb` (if not already running)

Use a **wide and tall** screen. The bot uses a **layout viewport** (default **1920×1200**) plus a **taller Chromium window** (`--window-size` adds **~220px** by default for tabs/toolbar above the page). **Xvfb must be at least as big as that outer window** (roughly **1920 × 1420** with defaults), or the bottom of the page (DM composer) will still look “cut off”.

```bash
# Only if :98 is free — 1440 height leaves a little slack above min outer window:
Xvfb :98 -screen 0 1920x1440x24 &
sleep 1
```

Verify: `DISPLAY=:98 xdpyinfo | grep dimensions` → should show **1920x1440** (or whatever you chose).

If you already had `1280x800`, **stop** that Xvfb (`pkill Xvfb` — note: kills all Xvfb) and start again with a large mode, then restart `fluxbox` / `x11vnc` on `:98`.

### B3. Window manager (**skip for cleanest VNC + Chrome**)

**Problem you may see:** With **fluxbox**, the bottom **taskbar** steals height and windows sometimes **cascade** (not at `0,0`). Then the **tabs / URL bar** look “cut off” at the top even when VNC is scrolled to the top, and TigerVNC **scrollbars** appear because the total desktop is taller than Chrome alone.

**Recommended for Instagram / mic debugging — no fluxbox:**

- Do **not** start `fluxbox`. Use only **Xvfb + x11vnc**; Chromium is launched with **`--window-position=0,0`** (default in current `bot.js`) so the window hugs the top-left.
- Size **Xvfb** to fit **only** the Chromium outer window (no extra panel):

  `Xvfb_height ≥ DESKTOP_VIEWPORT_HEIGHT + DESKTOP_WINDOW_PAD_Y + ~30` (small slack)

  Example with **`DESKTOP_VIEWPORT_WIDTH=1512`**, **`HEIGHT=982`**, **`DESKTOP_WINDOW_PAD_Y=220`**:

  - Outer Chrome ≈ **1512 × 1202**
  - Use: `Xvfb :98 -screen 0 1512x1240x24` (a little slack)

**If you still want fluxbox** (other tools on the same display):

```bash
DISPLAY=:98 fluxbox &
sleep 1
```

Then add **extra Xvfb height** for the toolbar (~**40–80px**) and expect to **scroll** in TigerVNC unless the viewer is huge.

---

## Part C — `x11vnc` (Ubuntu: no `-securitytypes`)

Ubuntu’s `x11vnc` often **rejects** `-securitytypes` — do **not** use it.

```bash
x11vnc -display :98 -forever -shared -nopw -listen 127.0.0.1 -rfbport 5900 -noxdamage &
```

You should see lines like **“Listening for VNC connections on TCP port 5900”** and **“Using X display :98”**.

If port 5900 is busy:

```bash
pkill x11vnc; sleep 1
# then run the x11vnc line again
```

---

## Part D — Prove the display works (before the bot)

Still on the **VPS**:

```bash
DISPLAY=:98 xterm -geometry 80x24+80+80 -bg grey20 -fg white &
```

**Expected:** a grey terminal window should exist on display `:98` (you will see it in VNC in the next part).

Check that the X server has windows:

```bash
DISPLAY=:98 xwininfo -root -tree 2>/dev/null | head -30
```

You should see more than just an empty root (e.g. entries for `xterm`).

---

## Part E — SSH tunnel + TigerVNC (your Mac)

### E1. Tunnel (leave this terminal open)

```bash
ssh -L 5900:127.0.0.1:5900 root@YOUR_VPS_IP
```

Use your real user if not `root`.

### E2. TigerVNC Viewer

Connect to:

- **Host:** `127.0.0.1` or `localhost`  
- **Port:** `5900`  

(or `vnc://127.0.0.1:5900` in some clients)

**You should see the grey `xterm` from Part D.**  
If VNC connects but is **still** all black, go back to Part B/C/D — do **not** continue until `xterm` is visible.

### E2b — **No TigerVNC scrollbars** + see **tabs / URL bar**

TigerVNC shows **scrollbars** when the **remote framebuffer** (Xvfb) is **larger in pixels** than the **TigerVNC window’s inner area** on your Mac. That is independent of Instagram — you are panning the whole virtual monitor.

Do **all** of these for a 1:1, no-scroll setup:

1. **Shrink the remote desktop to “just Chrome”**  
   - Prefer **no fluxbox** (Part **B3**).  
   - Set `.env` viewport + pad, then Xvfb only **slightly** larger than Chromium’s outer size (see **B3** example: **1512×1240** for 1512×982 + 220 pad).

2. **Make the TigerVNC window physically big enough**  
   - **Full-screen** TigerVNC on the Mac (green button) or drag the window so its **content area** is at least **remote_width × remote_height** pixels.  
   - On **Retina**, “1512pt” wide can be **3024** physical pixels — VNC often maps **1 remote pixel ↔ 1 viewer pixel**, so a small floating window will always scroll. **Maximize / full screen** the viewer.

3. **If you still must scroll — scale the whole remote image**  
   - Depends on build: **View →** look for **Scaling**, **Zoom**, or **Fit to window**; or **TigerVNC → Preferences / Options → Display**. Goal: **downscale** the remote framebuffer so the **entire** Xvfb fits inside your window (may look softer).

4. **Tabs / URL bar “missing” at the top**  
   - Deploy latest `bot.js`: headed launch adds **`--window-position=0,0`** (override with **`CHROME_WINDOW_POSITION`** in `.env` if needed).  
   - **Remove fluxbox** or hide its toolbar so nothing shifts the window.  
   - Confirm Xvfb height ≥ outer window height (viewport + `DESKTOP_WINDOW_PAD_Y` + slack).

### E3. If TigerVNC says “no matching security types”

Use a VNC password instead of `-nopw`:

```bash
# On VPS:
pkill x11vnc; sleep 1
mkdir -p ~/.vnc
x11vnc -storepasswd ~/.vnc/passwd
x11vnc -display :98 -forever -shared -listen 127.0.0.1 -rfbport 5900 -rfbauth ~/.vnc/passwd -noxdamage &
```

Reconnect from Mac; enter the password you set.

---

## Part F — Bot: headed Chromium on the **same** display

### F1. `.env` on the VPS (in the app directory PM2 runs from)

Set **exactly** (string `false` matters):

```bash
HEADLESS_MODE=false
DISPLAY=:98
```

Save the file.

### F2. Restart PM2 so the process inherits these vars

```bash
cd /path/to/your/ColdDMs/repo
pm2 restart all
```

### F3. Confirm the **Node** process sees `DISPLAY` (optional)

```bash
pm2 show 0 | head -40
# or check your app's env in ecosystem / `pm2 env 0`
```

If `DISPLAY` is missing here, Chromium will not attach to Xvfb — fix `.env` location or PM2 config, then restart again.

---

## Part G — Open the manual debug browser (from your Mac)

The HTTP API listens on **`http://YOUR_VPS_IP:3000`** unless you set `DASHBOARD_PORT`.

```bash
curl -sS -X POST "http://YOUR_VPS_IP:3000/api/debug/follow-up/browser" \
  -H "Authorization: Bearer YOUR_COLD_DM_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"clientId":"PASTE_REAL_UUID","instagramSessionId":"PASTE_REAL_UUID"}'
```

Optional: open a DM thread (real username, no placeholder):

```json
{"clientId":"...","instagramSessionId":"...","recipientUsername":"someone"}
```

**Expected JSON:** `{"ok":true,"accepted":true,...}`

Then watch VNC — Instagram should appear within a few seconds.

### G1. Confirm Chromium is actually running

On the **VPS**:

```bash
pgrep -a chromium || pgrep -a chrome
pid=$(pgrep -fn chromium || pgrep -fn chrome)
if [ -n "$pid" ]; then
  tr '\0' '\n' < /proc/$pid/environ | grep '^DISPLAY='
fi
```

You want **`DISPLAY=:98`** (or whatever display you chose).

If **no** chromium process:

- `pm2 logs --lines 100` and search for `[debug] follow-up/browser`
- Common messages: session expired (login page), missing cookies, Supabase error, or **409** if a debug session is already “active” → **restart PM2** (Part A2) and call the endpoint again.

### G1b — Manual mic in DMs (“nothing happens” / popup at top)

- **Not because of “Chromium”** — Puppeteer uses **Chrome for Testing** (normal Chromium). Same WebRTC stack as Chrome.  
- **Permission “Allow” ≠ a working device.** On a bare VPS there is often **no real capture device**. `getUserMedia({ audio: true })` can **fail** even when site permissions say Allow; Instagram may then do **nothing** on mic click.  
- **Headless follow-up sends (VPS):** `bot.js` sets **`PULSE_SOURCE=<VOICE_NOTE_SINK>.monitor`** by default and **does not** enable the fake media device (real audio). **VNC / headed** debug: if the mic UI still does nothing, try **`CHROMIUM_USE_FAKE_MEDIA_DEVICE=true`** or confirm Pulse has the sink/monitor (`pactl list short sources`).  
- **Desktop Instagram** usually expects **press and hold** on the mic (~**0.5–1 s**), not a single click.  
- The bot re-runs **`overridePermissions(microphone)`** after load — check **`pm2 logs`** for **`[voice] Microphone permission granted`**.  
- To confirm errors: **DevTools → Console** (right‑click → Inspect) and click the mic; look for `getUserMedia` / `NotFoundError` / `NotReadableError`.  
- Real sends: PulseAudio null sink + monitor + automatic `PULSE_SOURCE` (see **`docs/FOLLOW_UP_SEND.md`**).

### G2. How long the window stays open

- If **`FOLLOW_UP_DEBUG_BROWSER_MS`** is **unset**, Chromium is held until **PM2 restart** (or the process exits).
- If set (e.g. `1800000`), it auto-closes after that many ms — then `pgrep chromium` may show nothing; that is normal.

---

## Part H — Quick troubleshooting

| Symptom | What to check |
|--------|----------------|
| DM **composer missing** / bottom cut off | Browser **chrome** (tabs, URL bar) sits **above** the page. The outer window must be **taller** than the viewport: defaults add **`DESKTOP_WINDOW_PAD_Y` (220px)** to `--window-size`. **Xvfb height ≥ viewport height + pad** (e.g. **1440** with defaults). Check logs: `viewport=… windowSize=…`. Increase **`DESKTOP_WINDOW_PAD_Y`** (e.g. `280`) if needed. |
| Instagram / Chromium **cut off on the right** | Widen **`DESKTOP_VIEWPORT_WIDTH`** (e.g. `2048`) and grow Xvfb width to match. Deploy latest `bot.js` so **`--window-size`** includes chrome padding. TigerVNC: **100%** scale, maximize viewer. |
| Black screen in VNC | Run Part D (`xterm`) again while VNC is connected. |
| `curl` fails to port 443 | Use **`http://IP:3000`**, not `https://IP` unless you have a reverse proxy. |
| `409` from debug endpoint | `pm2 restart all`, wait for online, curl again. |
| No `chromium` in `pgrep` | `pm2 logs`, session/login errors, or timed `FOLLOW_UP_DEBUG_BROWSER_MS`. |
| `x11vnc` “unrecognized `-securitytypes`” | Remove that flag; use Part C or E3. |
| Two `x11vnc` / bind errors | `pkill x11vnc`, start a single instance (Part C). |
| TigerVNC **scrollbars** / must pan | Remote Xvfb is **bigger** than the viewer window — full-screen TigerVNC, or **smaller Xvfb** (Part B3), or **viewer scaling** (Part E2b). |
| **Top of Chrome** (tabs) missing | **`--window-position=0,0`**, **no fluxbox** (Part B3), enough **Xvfb height** for outer `--window-size`. |

---

## Security

- Do **not** expose `x11vnc` on `0.0.0.0` without a strong password and firewall rules. **`127.0.0.1` + SSH tunnel** is the default safe pattern in this doc.
- Protect **`COLD_DM_API_KEY`** and **`POST /api/debug/follow-up/browser`** like any admin API.

---

## Related

- General VPS notes: **`DEPLOYMENT.md`** → *Watching the browser on a VPS*
- Debug endpoint summary: **`docs/FOLLOW_UP_SEND.md`** → *Manual browser only*
