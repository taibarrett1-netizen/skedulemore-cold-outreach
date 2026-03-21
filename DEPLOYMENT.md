# Deployment (VPS, 24/7)

Run the bot on a small Linux server so it can run 24/7. Same steps as in the README; consolidated here for reference.

## 1. Choose a server

- **Provider:** DigitalOcean, Linode, Vultr, etc. (e.g. DigitalOcean "Basic" $6/mo)
- **Image:** Ubuntu 22.04 LTS
- **Size:** 1 vCPU, 1–2 GB RAM
- **Region:** Any (latency to Instagram is fine from any region)

## 2. First login and basics

- SSH: `ssh root@YOUR_SERVER_IP` (or `ubuntu@...` if your provider gives an `ubuntu` user)
- Update: `sudo apt update && sudo apt upgrade -y`

## 3. Install Node.js (v20+)

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
node -v
npm -v
```

## 4. Install Chromium dependencies (for Puppeteer)

Puppeteer needs these on minimal Linux for headless Chrome:

```bash
sudo apt install -y libgbm1 libasound2 libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libpango-1.0-0 libcairo2
```

### Voice notes (required for voice follow-ups and cold-DM voice)

**`ffmpeg` and `ffprobe` must be installed** (voice pipeline runs `ffmpeg` to play audio into PulseAudio). If you see `spawn ffmpeg ENOENT` in logs, install:

```bash
sudo apt install -y ffmpeg
ffmpeg -version && ffprobe -version
```

Optional: set `FFMPEG_PATH` / `FFPROBE_PATH` in `.env` if the binaries are not on `PATH`.

Then configure PulseAudio virtual sink (for routing audio into the browser “mic”):

```bash
sudo apt install -y pulseaudio-utils pulseaudio
pactl load-module module-null-sink sink_name=ColdDMsVoice sink_properties=device.description=ColdDMsVoice
pactl list short sources | rg "ColdDMsVoice.*monitor"
```

Set these in `.env`:

- `VOICE_NOTE_FILE=/absolute/path/to/voice.wav`
- `VOICE_NOTE_MODE=after_text` or `voice_only`
- `VOICE_NOTE_SINK=ColdDMsVoice`
- `VOICE_NOTE_PULSE_SOURCE=ColdDMsVoice.monitor` (optional but recommended)

## 5. Get the project onto the server

Repo name is **ColdDMs**; on your Mac the folder may be **Cold DMs V1**. On the server, `cd` into whatever the folder is actually called there.

**Option A – Git**

```bash
cd ~
git clone https://github.com/taibarrett1-netizen/ColdDMs.git
cd ColdDMs
npm install
```

**Option B – SCP from your Mac**

```bash
# On your Mac (use your real path to the project):
scp -r "/Users/taibarrett/Documents/SkeduleMore/Dashboard/Cold DMs V1" user@SERVER_IP:~/cold-dm-bot

# On server:
cd ~/cold-dm-bot
npm install
```

No inbound firewall ports are needed for the bot; it only makes outbound connections to Instagram.

## 6. Configure on the server

- Create `.env` (same variables as local). Set `HEADLESS_MODE=true`.
- For voice notes, use WAV PCM mono 48kHz when possible; compressed formats also work but can be less reliable for waveform duration.
- Add `leads.csv` in the project folder (or set `LEADS_CSV` in `.env`).
- The SQLite DB at `database/bot.db` is created on first run; ensure the `database/` directory is writable.

## 7. Run with PM2

Start the **ig-dm-bot** worker once and keep it running. It is a single long-running process that serves all clients (when using Supabase).

```bash
npm install -g pm2
pm2 start cli.js --name ig-dm-bot -- --start
pm2 save
pm2 startup
# Run the command that pm2 startup prints (so the bot restarts on reboot)
```

**Always-on worker (Supabase):** The worker process should stay running. **Start** and **Stop** in the dashboard do **not** start or stop the process. They only set the per-client pause flag in the database (`cold_dm_control.pause`). Start = allow sending for that client (`pause = 0`). Stop = stop sending for that client (`pause = 1`). The worker keeps running and will skip clients that are paused; when there is no work for any client it sleeps 30–60s and re-checks. Start the worker once at deploy; the dashboard only toggles who is allowed to send.

**Useful commands**

- `pm2 status` – list processes
- `pm2 logs ig-dm-bot` – view logs
- `pm2 restart ig-dm-bot` – restart after code/config changes

## 8. Web dashboard (optional)

Run the dashboard on the same server so you can add leads and edit settings in the browser:

```bash
cd ~/ColdDMs
npm run dashboard
# Or with PM2: pm2 start server.js --name ig-dm-dashboard
```

Then open **http://YOUR_DROPLET_IP:3000**. To expose it on port 80 (optional), use Nginx as a reverse proxy to `http://127.0.0.1:3000`.

**Setter dashboard Connect:** The setter UI calls `POST /api/instagram/connect` on this server. You must be running `server.js` (dashboard) on the droplet and have the latest code that includes this route. In Supabase secrets, set `COLD_DM_VPS_URL` to `http://YOUR_DROPLET_IP:3000` (no path, no trailing slash).

## 9. Run entirely on the Droplet (reset and go)

Do this on the **Droplet only** (no need to run the bot on your Mac).

**1. SSH in**
```bash
ssh root@YOUR_DROPLET_IP
```

**2. Go to the project and get latest code**
```bash
cd ~/ColdDMs
git pull origin main
npm install
```

**3. Stop any old bot/dashboard**
```bash
pm2 stop ig-dm-bot
pm2 stop ig-dm-dashboard
# or: pm2 stop all
```

**4. Configure on the Droplet (if not done yet)**

- **Option A – Dashboard (easiest):** Start the dashboard first, then open it in your browser to set Instagram and leads:
  ```bash
  pm2 start server.js --name ig-dm-dashboard
  ```
  Open **http://YOUR_DROPLET_IP:3000** in your browser. If the page doesn’t load, open port 3000 (see step 6). In the dashboard: **Settings** → add Instagram username/password, save. **Leads** → paste usernames, save.

- **Option B – Manual:** Create `.env` and `leads.csv` on the server:
  ```bash
  nano ~/ColdDMs/.env
  ```
  Add (replace with your values):
  ```
  INSTAGRAM_USERNAME=your_username
  INSTAGRAM_PASSWORD=your_password
  DAILY_SEND_LIMIT=100
  MIN_DELAY_MINUTES=5
  MAX_DELAY_MINUTES=30
  HEADLESS_MODE=true
  ```
  Save (Ctrl+O, Enter, Ctrl+X). Then create leads:
  ```bash
  echo -e "username\nlead1\nlead2" > ~/ColdDMs/leads.csv
  ```

**5. Start the bot**
```bash
cd ~/ColdDMs
pm2 start cli.js --name ig-dm-bot -- --start
pm2 save
pm2 startup
# If pm2 startup says to run a command, run it.
```

**6. (Optional) Open port 3000 so you can use the dashboard from your browser**

- DigitalOcean: Droplet → Networking → Firewall → add rule: Inbound, TCP, port 3000.
- Or on the server: `sudo ufw allow 3000 && sudo ufw status` (if using ufw).

**7. Check it’s working**
```bash
pm2 status
pm2 logs ig-dm-bot --lines 80
```

You should see logs like: `Loaded X leads`, then `First send in X seconds`, then `Logged in to Instagram.`, then `Sent to @user…`. If you see `Setup failed` or a timeout, check `pm2 logs ig-dm-dashboard` for no errors, then `~/ColdDMs/logs/error.log` on the server for the bot. Common fixes: Chromium deps (step 4 in this doc), correct `.env`, and a working Instagram login (no 2FA that blocks automation, or use an app password).

---

## 10. Updating after you push to GitHub

On the VPS:

```bash
cd ~/ColdDMs
git pull origin main
npm install   # if package.json changed
pm2 restart ig-dm-bot
pm2 restart ig-dm-dashboard   # if you use it
```

**One-liner (no sqlite3 required):**

```bash
cd ~/ColdDMs && git pull origin main && npm install && pm2 restart ig-dm-bot ig-dm-dashboard && pm2 status
```
(If you only run the bot, PM2 will report “not found” for the other name; that’s fine.)

If `ig-dm-bot` shows **errored**, see why: `pm2 logs ig-dm-bot --lines 80`
