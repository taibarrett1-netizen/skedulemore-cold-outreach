/**
 * Instagram Web DM voice note: find mic control, record, send.
 * Desktop viewport is required (see applyDesktopEmulation).
 *
 * Chromium still requests microphone access for WebRTC/getUserMedia; on a VPS the
 * "mic" is typically PulseAudio (virtual sink). Grant permission in the browser so
 * no OS dialog blocks automation (Safari dialogs are not in the page DOM).
 */

const { closeDmComposerOverlays } = require('./instagram-modals');
const {
  captureFollowUpScreenshot,
  isFollowUpScreenshotsEnabled,
  captureFollowUpScreenshotWithMarkers,
} = require('./follow-up-screenshots');
const { startVoiceNotePlayback } = require('./voice-note-audio');

/** When not `false`, wait for thread DOM to change after Send (audio/list rows). Reduces false "sent ok". */
const VOICE_NOTE_STRICT_VERIFY = process.env.VOICE_NOTE_STRICT_VERIFY !== 'false';

/** Max wait after mic click for Instagram recording strip (blue bar / 0:xx timer). */
const VOICE_RECORDING_UI_TIMEOUT_MS = Math.min(
  Math.max(parseInt(process.env.VOICE_RECORDING_UI_TIMEOUT_MS, 10) || 12000, 3000),
  45000
);

/** Puppeteer removed `page.waitForTimeout`; use this instead. */
function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Rough metrics before/after voice send. Instagram DM often does NOT use role=listitem/row or <audio>
 * until play, so we also use the scrollable message column above the composer (scrollHeight, children, text).
 */
function threadDomMetricsScript() {
  return function collectThreadDomMetrics() {
    const root =
      document.querySelector('section[role="main"]') ||
      document.querySelector('[role="main"]') ||
      document.querySelector('main') ||
      document.body;

    const lower = (s) => (s || '').toLowerCase();
    const mainSection =
      document.querySelector('section[role="main"]') ||
      document.querySelector('[role="main"]') ||
      document.querySelector('main');

    const findMessageScroller = () => {
      const inputs = document.querySelectorAll(
        'textarea[placeholder], textarea, p[contenteditable="true"], [contenteditable="true"], [role="textbox"], div[contenteditable="true"]'
      );
      for (const ta of inputs) {
        const ph = lower(ta.getAttribute('placeholder') || ta.getAttribute('aria-label') || '');
        if (!ph.includes('message') && !ph.includes('messenger')) {
          try {
            const r = ta.getBoundingClientRect();
            if (r.bottom < window.innerHeight * 0.25) continue;
          } catch {
            continue;
          }
        }
        let el = ta;
        for (let i = 0; i < 22 && el; i++) {
          el = el.parentElement;
          if (!el) break;
          try {
            const sh = el.scrollHeight;
            const ch = el.clientHeight || 0;
            if (sh > ch + 60 && sh > 120) return el;
          } catch {
            /* ignore */
          }
        }
      }
      /**
       * IG often nests the thread in a scrollable div that doesn’t sit directly above the composer.
       * If composer-walk fails, pick the largest overflow-y scroll area inside main (typical message column).
       */
      if (!mainSection) return null;
      let best = null;
      let bestSh = 0;
      try {
        mainSection.querySelectorAll('div').forEach((div) => {
          try {
            const st = window.getComputedStyle(div);
            const oy = st.overflowY;
            if (oy !== 'auto' && oy !== 'scroll' && oy !== 'overlay') return;
            const sh = div.scrollHeight;
            const ch = div.clientHeight || 0;
            if (ch < 100 || sh < ch + 40) return;
            const r = div.getBoundingClientRect();
            if (r.height < 80 || r.top > window.innerHeight * 0.92) return;
            if (sh > bestSh) {
              bestSh = sh;
              best = div;
            }
          } catch {
            /* ignore */
          }
        });
      } catch {
        /* ignore */
      }
      return best;
    };

    const scroller = findMessageScroller();
    let scrollerScrollHeight = 0;
    let scrollerChildCount = 0;
    let scrollerTextLen = 0;
    if (scroller) {
      try {
        scrollerScrollHeight = Math.round(scroller.scrollHeight);
        scrollerChildCount = scroller.children.length;
        scrollerTextLen = (scroller.innerText || '').length;
      } catch {
        /* ignore */
      }
    }

    const mainEl = document.querySelector('main') || root;
    let mainTextLen = 0;
    try {
      mainTextLen = (mainEl.innerText || '').length;
    } catch {
      /* ignore */
    }

    const audios = document.querySelectorAll('audio');
    const listItems = root.querySelectorAll('[role="listitem"]');
    const rows = root.querySelectorAll('[role="row"]');

    /** Voice rows often expose play/pause/voice aria before <audio> appears in DOM. */
    let mediaThreadHints = 0;
    const hintRoot = mainSection || root;
    try {
      hintRoot.querySelectorAll('div[role="button"], button, [role="button"]').forEach((el) => {
        try {
          const r = el.getBoundingClientRect();
          if (r.bottom > window.innerHeight - 100) return;
          const al = lower(el.getAttribute('aria-label') || '');
          const ti = lower(el.getAttribute('title') || '');
          const t = `${al} ${ti}`;
          if (
            t.includes('play') ||
            t.includes('pause') ||
            t.includes('voice') ||
            t.includes('clip') ||
            t.includes('audio message')
          ) {
            mediaThreadHints += 1;
          }
        } catch {
          /* ignore */
        }
      });
    } catch {
      /* ignore */
    }

    return {
      audio: audios.length,
      listItems: listItems.length,
      rows: rows.length,
      scrollerScrollHeight,
      scrollerChildCount,
      scrollerTextLen,
      mainTextLen,
      mediaThreadHints,
    };
  };
}

function voiceThreadLooksDelivered(before, after) {
  const scrollDelta = after.scrollerScrollHeight - before.scrollerScrollHeight;
  const childDelta = after.scrollerChildCount - before.scrollerChildCount;
  const scrollerTextDelta = after.scrollerTextLen - before.scrollerTextLen;
  const mainTextDelta = after.mainTextLen - before.mainTextLen;
  const hintDelta = (after.mediaThreadHints || 0) - (before.mediaThreadHints || 0);

  if (after.audio > before.audio) return true;
  if (after.listItems > before.listItems) return true;
  if (after.rows > before.rows) return true;
  /** New bubble often grows scroll area or adds a child row. */
  if (scrollDelta >= 25) return true;
  if (scrollDelta >= 8 && after.scrollerScrollHeight > 0) return true;
  if (childDelta >= 1) return true;
  /** New message text (e.g. duration label) inside the thread scroller. */
  if (before.scrollerTextLen > 0 && scrollerTextDelta >= 8) return true;
  if (before.scrollerScrollHeight > 0 && scrollerTextDelta >= 8) return true;
  /** Voice note row often adds a play/voice control in the thread. */
  if (hintDelta >= 1) return true;
  /** Fallback: main pane text grew (noisier; ignore tiny deltas). */
  if (mainTextDelta >= 12) return true;
  /** Voice-only bubble may add almost no innerText; allow tiny growth if scroller started working. */
  if (mainTextDelta >= 4 && after.scrollerScrollHeight > before.scrollerScrollHeight) return true;
  return false;
}

/**
 * After clicking Send, poll until thread metrics suggest a new message vs snapshot.
 */
async function waitForVoiceDeliveredInThread(page, before, opts = {}) {
  const { timeoutMs = 22000, pollMs = 450, logger = null } = opts;
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const after = await page.evaluate(threadDomMetricsScript()).catch(() => null);
    if (after && voiceThreadLooksDelivered(before, after)) {
      if (logger) {
        logger.log(
          `Voice verify: thread changed (audio ${before.audio}→${after.audio}, list ${before.listItems}→${after.listItems}, rows ${before.rows}→${after.rows}, scroll ${before.scrollerScrollHeight}→${after.scrollerScrollHeight}, scrollerKids ${before.scrollerChildCount}→${after.scrollerChildCount}, scrollerText ${before.scrollerTextLen}→${after.scrollerTextLen}, mainText ${before.mainTextLen}→${after.mainTextLen}, mediaHints ${before.mediaThreadHints ?? 0}→${after.mediaThreadHints ?? 0})`
        );
      }
      return true;
    }
    await delay(pollMs);
  }
  if (logger) {
    const snap = await page.evaluate(threadDomMetricsScript()).catch(() => before);
    logger.warn(
      `Voice verify: no thread change within ${timeoutMs}ms (final: audio=${snap.audio} listItems=${snap.listItems} rows=${snap.rows} scroll=${snap.scrollerScrollHeight} kids=${snap.scrollerChildCount} scrollerText=${snap.scrollerTextLen} mainText=${snap.mainTextLen} mediaHints=${snap.mediaThreadHints ?? 0}; before scroll=${before.scrollerScrollHeight} beforeHints=${before.mediaThreadHints ?? 0})`
    );
  }
  return false;
}

/**
 * Recording UI detection scoped to the **DM composer dock** (Message field row + strip above it).
 * Avoids false positives from **blue outgoing message bubbles** in the thread (old logic used any blue in lower half).
 */
function detectInstagramVoiceRecordingUiScript() {
  return function detectInstagramVoiceRecordingUi() {
    const lower = (s) => (s || '').toLowerCase();
    /** Match focusThreadComposer / mic layout — avoids false lastWhy=no_composer on non-English UI. */
    function findComposerForDock() {
      const byPlaceholder = (el) => {
        const p = (el.getAttribute && el.getAttribute('placeholder')) || '';
        const a = (el.getAttribute && el.getAttribute('aria-label')) || '';
        const t = (p + ' ' + a).toLowerCase();
        return (
          t.includes('message') ||
          t.includes('messenger') ||
          t.includes('add a message') ||
          t.includes('write a message')
        );
      };
      const all = document.querySelectorAll(
        'textarea, div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"], [role="textbox"]'
      );
      for (const el of all) {
        try {
          if (!el || el.disabled) continue;
          if (el.offsetParent === null && (!el.getClientRects || el.getClientRects().length === 0)) continue;
          if (byPlaceholder(el)) return el;
        } catch {
          /* ignore */
        }
      }
      for (const el of all) {
        try {
          if (!el || el.disabled) continue;
          if (el.offsetParent === null && (!el.getClientRects || el.getClientRects().length === 0)) continue;
          return el;
        } catch {
          /* ignore */
        }
      }
      return null;
    }

    const compose = findComposerForDock();
    if (!compose) {
      return { ok: false, why: 'no_composer' };
    }
    const cr = compose.getBoundingClientRect();

    /** Vertical: from just above the composer pill to bottom of viewport (not mid-thread bubbles). */
    const dockTop = Math.max(cr.top - 120, window.innerHeight * 0.62);
    const dockBottom = window.innerHeight + 4;
    /** Horizontal: align with composer column (exclude left nav / inbox list). */
    const dockLeft = Math.max(0, cr.left - 60);
    const dockRight = window.innerWidth - 4;

    const centerInDock = (r) => {
      const cx = r.left + r.width / 2;
      const cy = r.top + r.height / 2;
      return (
        cy >= dockTop &&
        cy <= dockBottom &&
        cx >= dockLeft &&
        cx <= dockRight
      );
    };

    /** Pause / delete recording — only in composer dock (IG often uses div[tabindex="0"], not role=button). */
    for (const el of document.querySelectorAll(
      '[aria-label], [title], button, [role="button"], div[tabindex], span[tabindex], a[tabindex]'
    )) {
      let r;
      try {
        r = el.getBoundingClientRect();
      } catch {
        continue;
      }
      if (!centerInDock(r)) continue;
      const al = lower(el.getAttribute('aria-label') || '');
      const ti = lower(el.getAttribute('title') || '');
      const t = `${al} ${ti}`;
      if (t.includes('pause') && (t.includes('record') || t.includes('recording'))) return { ok: true, why: 'aria_pause' };
      if (t.includes('delete') && (t.includes('clip') || t.includes('record'))) return { ok: true, why: 'aria_delete' };
    }

    /** Recording timer 0:xx / 1:xx — small text, in dock only (not header clock). */
    for (const el of document.querySelectorAll('div, span, p')) {
      let r;
      try {
        r = el.getBoundingClientRect();
      } catch {
        continue;
      }
      if (!centerInDock(r) || r.height <= 0 || r.height > 56) continue;
      const text = (el.textContent || '').trim();
      if (text.length > 6) continue;
      if (/^0:[0-5]\d$/.test(text)) return { ok: true, why: 'timer_0mm' };
      if (/^1:[0-5]\d$/.test(text)) return { ok: true, why: 'timer_1mm' };
    }

    /**
     * Blue “recording” strip: must hug the composer seam (outgoing bubbles also match RGB + wide+short).
     * Without strict placement we get false positives → ffmpeg runs while IG never left idle composer.
     */
    for (const el of document.querySelectorAll('div, span')) {
      let r;
      try {
        r = el.getBoundingClientRect();
      } catch {
        continue;
      }
      if (!centerInDock(r)) continue;
      if (r.width < 180 || r.height < 10 || r.height > 36) continue;
      if (r.width < r.height * 3) continue;
      /** Bottom edge of strip sits at/just above the message field top (not a bubble higher in the thread). */
      if (r.bottom > cr.top + 14 || r.top < cr.top - 52) continue;
      const bg = window.getComputedStyle(el).backgroundColor;
      const mm = bg.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
      if (!mm) continue;
      const R = +mm[1];
      const G = +mm[2];
      const B = +mm[3];
      if (B > 200 && R < 100 && G > 85) {
        return { ok: true, why: 'blue_strip_dock' };
      }
    }

    return { ok: false, why: 'none' };
  };
}

async function evaluateRecordingUiOnce(page) {
  return page.evaluate(detectInstagramVoiceRecordingUiScript()).catch(() => ({ ok: false, why: 'eval_error' }));
}

/**
 * After a mic gesture, wait up to `maxMs` for recording UI (tight polling).
 * @returns {Promise<{ ok: boolean, why?: string, method?: string }>}
 */
async function waitForRecordingUiAfterAttempt(page, maxMs, pollMs, logger, methodLabel) {
  const start = Date.now();
  let lastWhy = 'none';
  /** Require two matching polls so a one-frame false positive does not start ffmpeg. */
  let lastOkSig = '';
  let streak = 0;
  while (Date.now() - start < maxMs) {
    const res = await evaluateRecordingUiOnce(page);
    if (res && res.ok) {
      const sig = `${res.why}`;
      if (sig === lastOkSig) streak += 1;
      else {
        lastOkSig = sig;
        streak = 1;
      }
      if (streak >= 2) {
        if (logger) logger.log(`Voice: recording UI (${res.why}) after ${methodLabel} (confirmed x2)`);
        return { ok: true, why: res.why, method: methodLabel };
      }
    } else {
      lastOkSig = '';
      streak = 0;
      lastWhy = (res && res.why) || 'none';
    }
    await delay(pollMs);
  }
  if (logger) logger.log(`Voice: no recording UI after ${methodLabel} (lastWhy=${lastWhy})`);
  return { ok: false, why: lastWhy, method: methodLabel };
}

/** Ms to poll for recording UI after each mic gesture (env: VOICE_MIC_ATTEMPT_WAIT_MS, else tied to VOICE_RECORDING_UI_TIMEOUT_MS). */
const PER_ATTEMPT_RECORDING_WAIT_MS = Math.min(
  Math.max(
    parseInt(process.env.VOICE_MIC_ATTEMPT_WAIT_MS, 10) ||
      Math.min(4000, Math.max(1200, Math.floor(VOICE_RECORDING_UI_TIMEOUT_MS / 3))),
    800
  ),
  8000
);

/**
 * Desktop IG sometimes uses placeholders without the word "message" (i18n / A/B).
 * Keep in sync with focusThreadComposer() below.
 */
const VOICE_MIC_PRESS_HOLD_MS = Math.min(
  Math.max(parseInt(process.env.VOICE_MIC_PRESS_HOLD_MS, 10) || 210, 120),
  400
);

/** Longer press on mic to *start* recording (ms) when a normal click does not open the recording UI. */
const VOICE_MIC_START_HOLD_MS = Math.min(
  Math.max(parseInt(process.env.VOICE_MIC_START_HOLD_MS, 10) || 550, 350),
  2500
);

/**
 * Try several click paths; only proceed to the next if recording UI did not appear.
 */
async function activateMicUntilRecordingUi(page, micEl, cx, cy, logger) {
  const attempts = [
    {
      name: 'element.click',
      run: async () => {
        try {
          await micEl.click({ delay: 60 });
        } catch {
          await page.mouse.click(cx, cy, { delay: 40 });
        }
      },
    },
    {
      /**
       * Point at mic center and hold (mobile-style start). Desktop Web is usually tap-to-toggle, but
       * headless/IG sometimes only arms recording after a longer press.
       */
      name: 'mouse_hold_to_start_recording',
      run: async () => {
        await page.mouse.move(Math.round(cx), Math.round(cy));
        await delay(100);
        await page.mouse.down();
        await delay(VOICE_MIC_START_HOLD_MS);
        await page.mouse.up();
        await delay(80);
      },
    },
    {
      /** Stepped pointer path + short press-hold (desktop Web often responds better than instant click). */
      name: 'stepped_move+press_hold',
      run: async () => {
        const steps = 8;
        const offX = Math.min(130, Math.max(36, cx * 0.14));
        const offY = Math.min(90, Math.max(22, cy * 0.07));
        const startX = Math.max(4, cx - offX);
        const startY = Math.max(4, cy - offY);
        for (let i = 0; i <= steps; i++) {
          const t = i / steps;
          const jx = Math.random() * 5 - 2.5;
          const jy = Math.random() * 5 - 2.5;
          await page.mouse.move(
            Math.round(startX + (cx - startX) * t + jx),
            Math.round(startY + (cy - startY) * t + jy)
          );
          await delay(10 + Math.floor(Math.random() * 12));
        }
        await page.mouse.move(Math.round(cx), Math.round(cy));
        await delay(50 + Math.floor(Math.random() * 45));
        await page.mouse.down();
        await delay(VOICE_MIC_PRESS_HOLD_MS);
        await page.mouse.up();
      },
    },
    {
      name: 'mouse_move+down+up',
      run: async () => {
        await page.mouse.move(cx, cy);
        await delay(80);
        await page.mouse.down();
        await delay(100);
        await page.mouse.up();
      },
    },
    {
      name: 'mouse.click_coords',
      run: async () => {
        await page.mouse.move(cx, cy);
        await delay(50);
        await page.mouse.click(cx, cy, { delay: 55, clickCount: 1 });
      },
    },
    {
      name: 'elementFromPoint+pointer+mouse',
      run: async () => {
        const x = Math.round(cx);
        const y = Math.round(cy);
        await page.evaluate(
          (px, py) => {
            const target = document.elementFromPoint(px, py);
            if (!target) return;
            const common = { bubbles: true, cancelable: true, view: window, clientX: px, clientY: py };
            if (typeof PointerEvent !== 'undefined') {
              target.dispatchEvent(
                new PointerEvent('pointerdown', { ...common, pointerId: 1, pointerType: 'mouse', isPrimary: true })
              );
            }
            target.dispatchEvent(new MouseEvent('mousedown', common));
            if (typeof PointerEvent !== 'undefined') {
              target.dispatchEvent(
                new PointerEvent('pointerup', { ...common, pointerId: 1, pointerType: 'mouse', isPrimary: true })
              );
            }
            target.dispatchEvent(new MouseEvent('mouseup', common));
            target.dispatchEvent(new MouseEvent('click', common));
          },
          x,
          y
        );
      },
    },
  ];

  for (const a of attempts) {
    await a.run();
    const got = await waitForRecordingUiAfterAttempt(
      page,
      PER_ATTEMPT_RECORDING_WAIT_MS,
      180,
      logger,
      a.name
    );
    if (got.ok) return got;
    await delay(350);
  }
  return { ok: false, why: 'all_attempts_failed' };
}

/**
 * Grant mic for instagram.com so getUserMedia succeeds without a permission prompt.
 * Safe to call once per page/context.
 */
async function grantMicrophoneForInstagram(page) {
  const origins = ['https://www.instagram.com', 'https://instagram.com'];
  for (const origin of origins) {
    try {
      await page.browserContext().overridePermissions(origin, ['microphone']);
      return true;
    } catch {
      /* try next origin */
    }
  }
  return false;
}

function buildMicFinderScript() {
  return function findMicControl() {
    const lower = (s) => (s || '').toLowerCase().trim();

    const rectVisible = (el) => {
      if (!el) return false;
      try {
        const r = el.getBoundingClientRect();
        if (r.width < 2 || r.height < 2) return false;
        const st = window.getComputedStyle(el);
        if (st.visibility === 'hidden' || st.display === 'none' || parseFloat(st.opacity) === 0) return false;
        return true;
      } catch {
        return false;
      }
    };

    const textHints = (el) => {
      const parts = [
        el.getAttribute && el.getAttribute('aria-label'),
        el.getAttribute && el.getAttribute('title'),
        el.getAttribute && el.getAttribute('data-testid'),
        el.textContent,
      ];
      if (el.tagName === 'SVG' || (el.tagName && el.tagName.toLowerCase() === 'svg')) {
        parts.push(el.getAttribute && el.getAttribute('aria-label'));
      }
      return parts.map((p) => lower(p)).join(' ');
    };

    const isEmojiStickerNoise = (t) =>
      t.includes('emoji') ||
      t.includes('sticker') ||
      t.includes('gif') ||
      t.includes('expression') ||
      t.includes('smile');

    const matchesMic = (el) => {
      if (!rectVisible(el)) return false;
      const t = textHints(el);
      if (!t) return false;
      if (isEmojiStickerNoise(t)) return false;
      return (
        t.includes('microphone') ||
        t.includes('voice') ||
        t.includes('record') ||
        t.includes('audio') ||
        t.includes('clip') ||
        t.includes('memo') ||
        t.includes('hold to') ||
        /\bmic\b/.test(t)
      );
    };

    const selectors = [
      'button',
      'div[role="button"]',
      'span[role="button"]',
      'a[role="button"]',
      '[role="button"]',
      'svg[aria-label]',
    ];

    const seen = new Set();
    const candidates = [];
    for (const sel of selectors) {
      try {
        document.querySelectorAll(sel).forEach((el) => {
          if (!el || seen.has(el)) return;
          seen.add(el);
          candidates.push(el);
        });
      } catch {
        /* ignore */
      }
    }

    /** Desktop Web: [mic][gallery photo][sticker/heart] to the RIGHT of the "Message..." field only.
     *  Mic = leftmost of those three (NOT the emoji inside the bar on the left). */
    function findMicByComposerLayout() {
      function findComposerForDock() {
        const byPlaceholder = (el) => {
          const p = (el.getAttribute && el.getAttribute('placeholder')) || '';
          const a = (el.getAttribute && el.getAttribute('aria-label')) || '';
          const t = (p + ' ' + a).toLowerCase();
          return (
            t.includes('message') ||
            t.includes('messenger') ||
            t.includes('add a message') ||
            t.includes('write a message')
          );
        };
        const all = document.querySelectorAll(
          'textarea, div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"], [role="textbox"]'
        );
        for (const el of all) {
          try {
            if (!el || el.disabled) continue;
            if (el.offsetParent === null && (!el.getClientRects || el.getClientRects().length === 0)) continue;
            if (byPlaceholder(el)) return el;
          } catch {
            /* ignore */
          }
        }
        for (const el of all) {
          try {
            if (!el || el.disabled) continue;
            if (el.offsetParent === null && (!el.getClientRects || el.getClientRects().length === 0)) continue;
            return el;
          } catch {
            /* ignore */
          }
        }
        return null;
      }

      const compose = findComposerForDock();
      if (!compose) return null;

      const cr = compose.getBoundingClientRect();
      const bottomMinY = window.innerHeight - 180;

      const row = Array.from(document.querySelectorAll('button, div[role="button"], span[role="button"]'))
        .filter((el) => {
          if (!el.querySelector || !el.querySelector('svg')) return false;
          if (!rectVisible(el)) return false;
          const hint = textHints(el);
          if (hint && isEmojiStickerNoise(hint)) return false;
          const r = el.getBoundingClientRect();
          if (r.top < bottomMinY) return false;
          if (r.width < 8 || r.height < 8) return false;
          /** Must be to the right of the text field — excludes emoji on the left (small slop for IG layout) */
          if (r.left < cr.right - 12) return false;
          return true;
        })
        .sort((a, b) => a.getBoundingClientRect().left - b.getBoundingClientRect().left);

      if (row.length < 3) return null;
      const trio = row.slice(-3);
      const micCandidate = trio[0];
      if (micCandidate && rectVisible(micCandidate)) return micCandidate;
      return null;
    }

    const byLayout = findMicByComposerLayout();
    if (byLayout) return byLayout;

    const hit = candidates.find((el) => {
      if (!matchesMic(el)) return false;
      const r = el.getBoundingClientRect();
      if (r.top < window.innerHeight - 200) return false;
      if (r.left < window.innerWidth * 0.4) return false;
      return true;
    });
    if (hit) {
      if (hit.tagName === 'SVG' || (hit.tagName && hit.tagName.toLowerCase() === 'svg')) {
        const clickable = hit.closest('button, [role="button"], a') || hit.parentElement;
        return clickable && rectVisible(clickable) ? clickable : hit;
      }
      return hit;
    }

    return null;
  };
}

async function focusThreadComposer(page) {
  const composeHandle = await page.evaluateHandle(() => {
    const byPlaceholder = (el) => {
      const p = (el.getAttribute && el.getAttribute('placeholder')) || '';
      const a = (el.getAttribute && el.getAttribute('aria-label')) || '';
      const t = (p + ' ' + a).toLowerCase();
      return t.includes('message') || t.includes('add a message') || t.includes('write a message');
    };
    const all = document.querySelectorAll(
      'textarea, div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"], [role="textbox"]'
    );
    for (const el of all) {
      try {
        if (!el || el.disabled) continue;
        if (el.offsetParent === null && (!el.getClientRects || el.getClientRects().length === 0)) continue;
        if (byPlaceholder(el)) return el;
      } catch {
        /* ignore */
      }
    }
    for (const el of all) {
      try {
        if (!el || el.disabled) continue;
        if (el.offsetParent === null && (!el.getClientRects || el.getClientRects().length === 0)) continue;
        return el;
      } catch {
        /* ignore */
      }
    }
    return null;
  });

  const compose = composeHandle.asElement();
  if (!compose) {
    await composeHandle.dispose().catch(() => {});
    return false;
  }
  await compose.click({ delay: 40 }).catch(() => {});
  await compose.dispose().catch(() => {});
  await composeHandle.dispose().catch(() => {});
  return true;
}

/**
 * Click composer, wait until a mic/voice control appears.
 * Call **before** starting ffmpeg so audio lines up with recording.
 */
async function prepareVoiceNoteUi(page, opts = {}) {
  const { logger = null, timeoutMs = 18000 } = opts;
  const finder = buildMicFinderScript();

  await closeDmComposerOverlays(page);
  await focusThreadComposer(page).catch(() => {});
  await delay(350);

  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const h = await page.evaluateHandle(finder).catch(() => null);
    const el = h && h.asElement();
    if (el) {
      await el.dispose().catch(() => {});
      await h.dispose().catch(() => {});
      if (logger) logger.log('Voice recorder control visible in composer');
      return { ok: true };
    }
    if (h) await h.dispose().catch(() => {});
    await focusThreadComposer(page).catch(() => {});
    await delay(450);
  }

  return { ok: false, reason: 'voice_mic_not_found' };
}

/**
 * Resolve (and optionally click) the voice-note Send control. Single implementation for click + debug coords.
 * @param {{ click: boolean }} opts
 */
function voiceSendResolveScript(opts) {
  const doClick = !!(opts && opts.click);
  return function voiceSendResolve() {
    const lower = (s) => (s || '').toLowerCase();
    const visible = (el) => {
      if (!el) return false;
      try {
        const r = el.getBoundingClientRect();
        return r.width > 2 && r.height > 2;
      } catch {
        return false;
      }
    };
    const inStickerNoise = (el) => {
      let p = el;
      for (let i = 0; i < 8 && p; i++) {
        const t = (p.textContent || '').toLowerCase();
        if (t.includes('gif') && t.includes('sticker')) return true;
        p = p.parentElement;
      }
      return false;
    };

    function findComposerForDock() {
      const byPlaceholder = (el) => {
        const p = (el.getAttribute && el.getAttribute('placeholder')) || '';
        const a = (el.getAttribute && el.getAttribute('aria-label')) || '';
        const t = (p + ' ' + a).toLowerCase();
        return (
          t.includes('message') ||
          t.includes('messenger') ||
          t.includes('add a message') ||
          t.includes('write a message')
        );
      };
      const all = document.querySelectorAll(
        'textarea, div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"], [role="textbox"]'
      );
      for (const el of all) {
        try {
          if (!el || el.disabled) continue;
          if (el.offsetParent === null && (!el.getClientRects || el.getClientRects().length === 0)) continue;
          if (byPlaceholder(el)) return el;
        } catch {
          /* ignore */
        }
      }
      for (const el of all) {
        try {
          if (!el || el.disabled) continue;
          if (el.offsetParent === null && (!el.getClientRects || el.getClientRects().length === 0)) continue;
          return el;
        } catch {
          /* ignore */
        }
      }
      return null;
    }

    /** Do not treat Like/Heart/Gallery/Mic as Send (old fallback: rightmost bottom icon = heart). */
    const isDefinitelyNotSend = (label, title, txt) => {
      const t = `${label} ${title} ${txt}`;
      return (
        /\blike\b|\blove\b|\bheart\b|\breact\b|\bliking\b/.test(t) ||
        /\bemoji\b|\bsticker\b|\bgif\b|\bgallery\b|\bphoto\b|\bimage\b|\bcamera\b|\battach\b|\bclip\b\s*art/.test(t) ||
        /\bmicrophone\b|\bmic\b|\bvoice message\b|\brecord\b|\brecording\b|\bpause\b|\bdelete\b|\btrash\b/.test(t)
      );
    };

    const compose = findComposerForDock();
    if (!compose) {
      return { ok: false, why: 'no_composer', dockedCount: 0 };
    }
    const cr = compose.getBoundingClientRect();
    const dockTop = Math.max(cr.top - 160, window.innerHeight * 0.52);
    const dockBottom = window.innerHeight + 4;
    const dockLeft = Math.max(0, cr.left - 80);
    const dockRight = window.innerWidth - 4;

    const centerInDock = (el) => {
      let r;
      try {
        r = el.getBoundingClientRect();
      } catch {
        return false;
      }
      const cx = r.left + r.width / 2;
      const cy = r.top + r.height / 2;
      return cx >= dockLeft && cx <= dockRight && cy >= dockTop && cy <= dockBottom;
    };

    /** IG composer icons are often focusable divs without role="button" → docked was 0 before. */
    const clickSet = new Set();
    for (const sel of [
      'button',
      '[role="button"]',
      'a[role="button"]',
      'div[tabindex="0"]',
      'span[tabindex="0"]',
      'a[tabindex="0"]',
    ]) {
      try {
        document.querySelectorAll(sel).forEach((n) => clickSet.add(n));
      } catch {
        /* ignore */
      }
    }
    const clickables = Array.from(clickSet);

    const docked = clickables.filter((el) => visible(el) && !inStickerNoise(el) && centerInDock(el));

    const finish = (el, via) => {
      let r;
      try {
        r = el.getBoundingClientRect();
      } catch {
        return { ok: false, why: 'bad_rect', dockedCount: docked.length };
      }
      const label = (el.getAttribute && el.getAttribute('aria-label')) || '';
      const out = {
        ok: true,
        via,
        label: label.slice(0, 120),
        x: r.left + r.width / 2,
        y: r.top + r.height / 2,
        dockedCount: docked.length,
      };
      if (doClick) {
        try {
          el.click();
        } catch {
          return { ok: false, why: 'click_throw', dockedCount: docked.length };
        }
      }
      return out;
    };

    for (const el of docked) {
      const tid = lower(el.getAttribute('data-testid') || '');
      if (tid.includes('send') && tid.includes('voice')) {
        const res = finish(el, 'dock_data_testid_voice_send');
        if (res.ok) return res;
      }
    }
    for (const el of clickables) {
      if (!visible(el) || inStickerNoise(el)) continue;
      const tid = lower(el.getAttribute('data-testid') || '');
      if (!tid.includes('send')) continue;
      try {
        const r = el.getBoundingClientRect();
        const cy = r.top + r.height / 2;
        if (cy < dockTop - 20 || cy > dockBottom) continue;
      } catch {
        continue;
      }
      const lab = lower(el.getAttribute('aria-label') || '');
      if (isDefinitelyNotSend(lab, '', '')) continue;
      const res = finish(el, 'dock_data_testid_send');
      if (res.ok) return res;
    }

    for (const el of docked) {
      const label = lower(el.getAttribute('aria-label') || '');
      const title = lower(el.getAttribute('title') || '');
      const txt = lower((el.textContent || '').trim().slice(0, 40));
      if (isDefinitelyNotSend(label, title, txt)) continue;
      if (
        (label.includes('voice') && label.includes('send')) ||
        (title.includes('voice') && title.includes('send')) ||
        label.includes('send voice') ||
        label === 'send' ||
        title === 'send'
      ) {
        const res = finish(el, 'dock_aria_voice_or_send');
        if (res.ok) return res;
      }
    }

    for (const el of docked) {
      const label = lower(el.getAttribute('aria-label') || '');
      const title = lower(el.getAttribute('title') || '');
      const txt = lower((el.textContent || '').trim());
      if (isDefinitelyNotSend(label, title, txt)) continue;
      if (label.includes('sticker') || label.includes('gif') || label.includes('emoji')) continue;
      if (label.includes('send') || title.includes('send') || txt === 'send') {
        const res = finish(el, 'dock_aria_send_generic');
        if (res.ok) return res;
      }
    }

    /**
     * Recording strip: allow slight vertical overlap with composer row (IG layout varies).
     */
    const stripCandidates = docked
      .filter((el) => {
        if (!el.querySelector || !el.querySelector('svg')) return false;
        const label = lower(el.getAttribute('aria-label') || '');
        const title = lower(el.getAttribute('title') || '');
        const txt = lower((el.textContent || '').trim().slice(0, 24));
        if (isDefinitelyNotSend(label, title, txt)) return false;
        const r = el.getBoundingClientRect();
        if (r.width < 14 || r.height < 14 || r.width > 96 || r.height > 96) return false;
        const cy = r.top + r.height / 2;
        if (cy > cr.top + 30) return false;
        return true;
      })
      .sort((a, b) => b.getBoundingClientRect().right - a.getBoundingClientRect().right);

    if (stripCandidates.length) {
      const res = finish(stripCandidates[0], 'dock_rightmost_recording_strip');
      if (res.ok) return res;
    }

    /** Composer band: rightmost small SVG (paper plane often has empty aria). */
    const bandCandidates = docked
      .filter((el) => {
        if (!el.querySelector || !el.querySelector('svg')) return false;
        const label = lower(el.getAttribute('aria-label') || '');
        const title = lower(el.getAttribute('title') || '');
        const txt = lower((el.textContent || '').trim().slice(0, 24));
        if (isDefinitelyNotSend(label, title, txt)) return false;
        const r = el.getBoundingClientRect();
        if (r.width < 14 || r.height < 14 || r.width > 88 || r.height > 88) return false;
        const cy = r.top + r.height / 2;
        if (cy < cr.top - 22 || cy > cr.top + 44) return false;
        if (r.left < cr.left - 20) return false;
        return true;
      })
      .sort((a, b) => b.getBoundingClientRect().right - a.getBoundingClientRect().right);

    if (bandCandidates.length) {
      const res = finish(bandCandidates[0], 'dock_rightmost_composer_band');
      if (res.ok) return res;
    }

    return { ok: false, why: 'no_send_control_in_dock', dockedCount: docked.length };
  };
}

function clickSendAfterRecordingScript() {
  return voiceSendResolveScript({ click: true });
}

function voiceSendTargetPreviewScript() {
  return voiceSendResolveScript({ click: false });
}

/**
 * Desktop Chrome: one click on the mic starts recording; wait for audio duration; click Send (paper plane).
 * Mobile web: press-and-hold on the mic for the duration (older mobile IG pattern).
 */
async function sendVoiceNoteInThread(page, opts = {}) {
  const {
    holdMs: holdMsOpt = null,
    logger = null,
    correlationId = '',
    strictVerify = VOICE_NOTE_STRICT_VERIFY,
    /** When set, ffmpeg → Pulse starts only after recording UI is confirmed (desktop) or at press-start (mobile). */
    voiceSource = null,
  } = opts;
  const shotMeta = { correlationId, logger };

  if (!voiceSource && (holdMsOpt == null || holdMsOpt < 400)) {
    return { ok: false, reason: 'voice_note_failed' };
  }

  let internalPlayback = null;
  try {
    await closeDmComposerOverlays(page);

    const metricsBefore = await page.evaluate(threadDomMetricsScript()).catch(() => ({
      audio: 0,
      listItems: 0,
      rows: 0,
      scrollerScrollHeight: 0,
      scrollerChildCount: 0,
      scrollerTextLen: 0,
      mainTextLen: 0,
      mediaThreadHints: 0,
    }));
    if (logger) {
      logger.log(
        `Voice: thread snapshot before mic (audio=${metricsBefore.audio}, listItems=${metricsBefore.listItems}, rows=${metricsBefore.rows}, scroll=${metricsBefore.scrollerScrollHeight || 0}, mediaHints=${metricsBefore.mediaThreadHints ?? 0})`
      );
    }

    const finder = buildMicFinderScript();
    const viewport = page.viewport();
    const desktopFlow = !!(viewport && viewport.isMobile === false);

    const micHandle = await page.evaluateHandle(finder);
    const micEl = micHandle.asElement();
    if (!micEl) {
      await micHandle.dispose().catch(() => {});
      return { ok: false, reason: 'voice_mic_not_found' };
    }

    await micEl.scrollIntoViewIfNeeded().catch(() => {});
    const box = await micEl.boundingBox();
    if (!box) {
      await micEl.dispose().catch(() => {});
      await micHandle.dispose().catch(() => {});
      return { ok: false, reason: 'voice_mic_not_found' };
    }

    const cx = box.x + box.width / 2;
    const cy = box.y + box.height / 2;

    const afterShotMs = 600;
    let effectiveHoldMs = holdMsOpt || 7000;

    if (desktopFlow) {
      if (logger) {
        logger.log(
          'Voice (desktop): mic target screenshot → focus composer → try click methods (with composer-scoped recording check between each)'
        );
      }
      if (isFollowUpScreenshotsEnabled()) {
        await captureFollowUpScreenshotWithMarkers(
          page,
          [{ x: cx, y: cy, label: 'mic click target (red crosshair)' }],
          'voice-mic-click-target',
          shotMeta
        );
      }

      await page
        .evaluate(() => {
          const byPlaceholder = (el) => {
            const p = (el.getAttribute && el.getAttribute('placeholder')) || '';
            const a = (el.getAttribute && el.getAttribute('aria-label')) || '';
            const t = (p + ' ' + a).toLowerCase();
            return (
              t.includes('message') ||
              t.includes('messenger') ||
              t.includes('add a message') ||
              t.includes('write a message')
            );
          };
          const all = document.querySelectorAll(
            'textarea, div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"], [role="textbox"]'
          );
          for (const el of all) {
            try {
              if (!el || el.disabled) continue;
              if (el.offsetParent === null && (!el.getClientRects || el.getClientRects().length === 0)) continue;
              if (byPlaceholder(el)) {
                el.focus();
                return;
              }
            } catch {
              /* ignore */
            }
          }
          for (const el of all) {
            try {
              if (!el || el.disabled) continue;
              if (el.offsetParent === null && (!el.getClientRects || el.getClientRects().length === 0)) continue;
              el.focus();
              return;
            } catch {
              /* ignore */
            }
          }
        })
        .catch(() => {});

      const act = await activateMicUntilRecordingUi(page, micEl, cx, cy, logger);
      if (!act.ok) {
        if (isFollowUpScreenshotsEnabled()) {
          await captureFollowUpScreenshotWithMarkers(
            page,
            [{ x: cx, y: cy, label: 'all mic gestures failed — no recording UI in composer dock' }],
            'voice-recording-ui-missed',
            shotMeta
          );
        }
        await micEl.dispose().catch(() => {});
        await micHandle.dispose().catch(() => {});
        return { ok: false, reason: 'voice_recording_ui_not_detected' };
      }

      /** Plain PNG right after recording UI is detected (see blue bar / timer vs DOM heuristic). */
      if (isFollowUpScreenshotsEnabled()) {
        await delay(220);
        await captureFollowUpScreenshot(page, 'voice-recording-ui-just-confirmed', shotMeta);
      }

      if (voiceSource) {
        internalPlayback = startVoiceNotePlayback(
          voiceSource.path,
          voiceSource.sink || 'ColdDMsVoice',
          logger
        );
        effectiveHoldMs = Math.round(internalPlayback.durationSec * 1000 + 700);
      }

      if (logger) logger.log(`Voice (desktop): hold recording ~${Math.round(effectiveHoldMs)} ms, then send`);
      await delay(afterShotMs);
      if (isFollowUpScreenshotsEnabled()) {
        /** Mic crosshair shortly after recording starts (may differ from just-confirmed if UI flickers). */
        await captureFollowUpScreenshotWithMarkers(
          page,
          [{ x: cx, y: cy, label: 'mic click point (reference)' }],
          'voice-after-mic-click',
          shotMeta
        );
      }
      const remainingHold = Math.max(0, effectiveHoldMs - afterShotMs);
      if (isFollowUpScreenshotsEnabled() && remainingHold > 4500) {
        const half = Math.floor(remainingHold / 2);
        await delay(half);
        await captureFollowUpScreenshot(page, 'voice-recording-mid-hold', shotMeta);
        await delay(remainingHold - half);
      } else {
        await delay(remainingHold);
      }
    } else {
      if (voiceSource) {
        internalPlayback = startVoiceNotePlayback(
          voiceSource.path,
          voiceSource.sink || 'ColdDMsVoice',
          logger
        );
        effectiveHoldMs = Math.round(internalPlayback.durationSec * 1000 + 700);
      }
      if (logger) logger.log(`Voice (mobile web): press-and-hold ${Math.round(effectiveHoldMs)} ms`);
      if (isFollowUpScreenshotsEnabled()) {
        await captureFollowUpScreenshotWithMarkers(
          page,
          [{ x: cx, y: cy, label: 'mic press-and-hold target' }],
          'voice-mic-click-target',
          shotMeta
        );
      }
      await page.mouse.move(cx, cy);
      await page.mouse.down();
      await delay(effectiveHoldMs);
      await page.mouse.up();
    }

    /**
     * Stop ffmpeg → Pulse **before** clicking Send. Leaving the stream running made the “recording”
     * session ambiguous and matched mis-clicks (e.g. heart) that never queued a voice message.
     */
    if (internalPlayback) {
      if (logger) logger.log('Voice: stopping ffmpeg playback before Send click');
      internalPlayback.stop();
      internalPlayback = null;
    }

    await micEl.dispose().catch(() => {});
    await micHandle.dispose().catch(() => {});

    /** Do NOT send Escape here — it dismisses Instagram's voice recording UI before Send. */
    await delay(1200);

    const previewSend = voiceSendTargetPreviewScript();
    if (isFollowUpScreenshotsEnabled()) {
      await delay(280);
      await captureFollowUpScreenshot(page, 'voice-after-playback-before-send', shotMeta);
      const preview = await page.evaluate(previewSend).catch(() => null);
      if (preview && preview.ok && Number.isFinite(preview.x) && Number.isFinite(preview.y)) {
        if (logger) {
          logger.log(
            `Voice: debug Send target preview (${preview.via}) dockedButtons=${preview.dockedCount ?? '?'} aria="${(preview.label || '').slice(0, 80)}"`
          );
        }
        await captureFollowUpScreenshotWithMarkers(
          page,
          [
            {
              x: preview.x,
              y: preview.y,
              label: `Send target (${preview.via}) — Puppeteer will click here`,
            },
          ],
          'voice-send-click-target',
          shotMeta
        );
      } else {
        const why = preview && preview.why ? preview.why : 'preview_eval_failed';
        const dc = preview && typeof preview.dockedCount === 'number' ? preview.dockedCount : '?';
        if (logger) logger.warn(`Voice: could not resolve Send coords for screenshot (${why}, docked=${dc})`);
        const vp = page.viewport() || { width: 1280, height: 800 };
        await captureFollowUpScreenshotWithMarkers(
          page,
          [
            {
              x: Math.min(vp.width - 48, Math.max(80, (vp.width * 3) / 4)),
              y: vp.height - 90,
              label: 'Send not resolved — inspect voice-after-playback-before-send',
            },
          ],
          'voice-send-target-unresolved',
          shotMeta
        );
      }
    }

    const clickSend = clickSendAfterRecordingScript();
    let sendResult = null;
    for (let attempt = 0; attempt < 15; attempt++) {
      sendResult = await page.evaluate(clickSend).catch(() => ({ ok: false, why: 'eval_error' }));
      if (sendResult && sendResult.ok) break;
      await delay(450);
    }
    if (!sendResult || !sendResult.ok) {
      const why = sendResult && sendResult.why ? sendResult.why : 'unknown';
      const dc = sendResult && typeof sendResult.dockedCount === 'number' ? sendResult.dockedCount : '';
      if (logger) {
        logger.warn(
          `Voice: could not click Send (${why}) after retries${dc !== '' ? ` dockedButtons=${dc}` : ''}`
        );
      }
      return { ok: false, reason: 'voice_send_button_not_found' };
    }

    if (logger) {
      logger.log(
        `Voice: send control clicked (${sendResult.via}) aria="${(sendResult.label || '').replace(/"/g, "'")}"; waiting for thread to update…`
      );
    }
    await delay(800);

    if (strictVerify) {
      const verified = await waitForVoiceDeliveredInThread(page, metricsBefore, { logger });
      if (!verified) {
        return { ok: false, reason: 'voice_not_confirmed_in_thread' };
      }
    } else if (logger) {
      logger.log('Voice: VOICE_NOTE_STRICT_VERIFY=false — skipping post-send DOM check');
    }

    await delay(400);
    return { ok: true };
  } finally {
    if (internalPlayback) internalPlayback.stop();
  }
}

module.exports = {
  sendVoiceNoteInThread,
  prepareVoiceNoteUi,
  grantMicrophoneForInstagram,
  VOICE_NOTE_STRICT_VERIFY,
};
