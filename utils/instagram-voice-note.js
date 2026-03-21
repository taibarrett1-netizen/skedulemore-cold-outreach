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
    const findMessageScroller = () => {
      const inputs = document.querySelectorAll(
        'textarea[placeholder], textarea, [contenteditable="true"], [role="textbox"], div[contenteditable="true"]'
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
        for (let i = 0; i < 18 && el; i++) {
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
      return null;
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

    return {
      audio: audios.length,
      listItems: listItems.length,
      rows: rows.length,
      scrollerScrollHeight,
      scrollerChildCount,
      scrollerTextLen,
      mainTextLen,
    };
  };
}

function voiceThreadLooksDelivered(before, after) {
  const scrollDelta = after.scrollerScrollHeight - before.scrollerScrollHeight;
  const childDelta = after.scrollerChildCount - before.scrollerChildCount;
  const scrollerTextDelta = after.scrollerTextLen - before.scrollerTextLen;
  const mainTextDelta = after.mainTextLen - before.mainTextLen;

  if (after.audio > before.audio) return true;
  if (after.listItems > before.listItems) return true;
  if (after.rows > before.rows) return true;
  /** New bubble often grows scroll area or adds a child row. */
  if (scrollDelta >= 25) return true;
  if (childDelta >= 1) return true;
  /** New message text (e.g. duration label) inside the thread scroller. */
  if (before.scrollerTextLen > 0 && scrollerTextDelta >= 8) return true;
  if (before.scrollerScrollHeight > 0 && scrollerTextDelta >= 8) return true;
  /** Fallback: main pane text grew (noisier; ignore tiny deltas). */
  if (mainTextDelta >= 12) return true;
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
          `Voice verify: thread changed (audio ${before.audio}→${after.audio}, list ${before.listItems}→${after.listItems}, rows ${before.rows}→${after.rows}, scroll ${before.scrollerScrollHeight}→${after.scrollerScrollHeight}, scrollerKids ${before.scrollerChildCount}→${after.scrollerChildCount}, scrollerText ${before.scrollerTextLen}→${after.scrollerTextLen}, mainText ${before.mainTextLen}→${after.mainTextLen})`
        );
      }
      return true;
    }
    await delay(pollMs);
  }
  if (logger) {
    const snap = await page.evaluate(threadDomMetricsScript()).catch(() => before);
    logger.warn(
      `Voice verify: no thread change within ${timeoutMs}ms (final: audio=${snap.audio} listItems=${snap.listItems} rows=${snap.rows} scroll=${snap.scrollerScrollHeight} kids=${snap.scrollerChildCount} scrollerText=${snap.scrollerTextLen} mainText=${snap.mainTextLen}; before scroll=${before.scrollerScrollHeight})`
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
    const inputs = Array.from(
      document.querySelectorAll('textarea[placeholder], [contenteditable="true"], [role="textbox"]')
    );
    const compose = inputs.find((el) => {
      const ph = lower(el.getAttribute('placeholder') || '');
      const al = lower(el.getAttribute('aria-label') || '');
      return ph.includes('message') || al.includes('message');
    });
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

    /** Pause / delete recording — only in composer dock. */
    for (const el of document.querySelectorAll('[aria-label], [title], button, [role="button"]')) {
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
     * Blue recording strip: must sit in dock, be **wider than tall** (not a bubble), bounded height.
     */
    for (const el of document.querySelectorAll('div, span')) {
      let r;
      try {
        r = el.getBoundingClientRect();
      } catch {
        continue;
      }
      if (!centerInDock(r)) continue;
      if (r.width < 160 || r.height < 12 || r.height > 64) continue;
      if (r.width < r.height * 2.2) continue;
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
  while (Date.now() - start < maxMs) {
    const res = await evaluateRecordingUiOnce(page);
    if (res && res.ok) {
      if (logger) logger.log(`Voice: recording UI (${res.why}) after ${methodLabel}`);
      return { ok: true, why: res.why, method: methodLabel };
    }
    lastWhy = (res && res.why) || 'none';
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
      const inputs = Array.from(
        document.querySelectorAll('textarea[placeholder], [contenteditable="true"], [role="textbox"]')
      );
      const compose = inputs.find((el) => {
        const ph = lower(el.getAttribute('placeholder') || '');
        const al = lower(el.getAttribute('aria-label') || '');
        return ph.includes('message') || al.includes('message');
      });
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

function clickSendAfterRecordingScript() {
  return function clickSendVoice() {
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
    const clickables = Array.from(
      document.querySelectorAll('button, div[role="button"], span[role="button"], a[role="button"]')
    );
    const voiceSend = clickables.find((el) => {
      if (!visible(el) || inStickerNoise(el)) return false;
      const label = lower(el.getAttribute('aria-label'));
      const title = lower(el.getAttribute('title'));
      return (
        (label.includes('voice') && label.includes('send')) ||
        (title.includes('voice') && title.includes('send')) ||
        label === 'send' ||
        label.includes('send voice')
      );
    });
    if (voiceSend) {
      voiceSend.click();
      return true;
    }
    const send = clickables.find((el) => {
      if (!visible(el) || inStickerNoise(el)) return false;
      const label = lower(el.getAttribute('aria-label'));
      const title = lower(el.getAttribute('title'));
      const txt = lower((el.textContent || '').trim());
      if (label.includes('sticker') || label.includes('gif') || label.includes('emoji')) return false;
      if (label.includes('send') || title.includes('send') || txt === 'send') return true;
      return false;
    });
    if (send) {
      send.click();
      return true;
    }
    const bottom = window.innerHeight - 180;
    const planes = clickables
      .filter((el) => {
        if (!visible(el) || inStickerNoise(el)) return false;
        if (!el.querySelector('svg')) return false;
        const label = lower(el.getAttribute('aria-label'));
        if (label.includes('sticker') || label.includes('gif') || label.includes('emoji')) return false;
        const r = el.getBoundingClientRect();
        return r.top >= bottom;
      })
      .sort((a, b) => b.getBoundingClientRect().right - a.getBoundingClientRect().right);
    if (planes.length && planes[0]) {
      planes[0].click();
      return true;
    }
    /** Recording bar: Send is usually the rightmost circular control above the composer. */
    const bottomY = window.innerHeight - 240;
    const row = clickables
      .filter((el) => {
        if (!visible(el) || inStickerNoise(el)) return false;
        if (!el.querySelector('svg')) return false;
        const r = el.getBoundingClientRect();
        if (r.top < bottomY) return false;
        const label = lower(el.getAttribute('aria-label'));
        if (label.includes('microphone') || label.includes('voice message') || label.includes('gallery')) return false;
        return r.width > 16 && r.height > 16;
      })
      .sort((a, b) => b.getBoundingClientRect().right - a.getBoundingClientRect().right);
    if (row.length) {
      row[0].click();
      return true;
    }
    return false;
  };
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
    }));
    if (logger) {
      logger.log(
        `Voice: thread snapshot before mic (audio=${metricsBefore.audio}, listItems=${metricsBefore.listItems}, rows=${metricsBefore.rows}, scroll=${metricsBefore.scrollerScrollHeight || 0})`
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
          const lower = (s) => (s || '').toLowerCase();
          const inputs = document.querySelectorAll('textarea, [contenteditable="true"], [role="textbox"]');
          for (const el of inputs) {
            const ph = lower(el.getAttribute('placeholder') || '');
            const al = lower(el.getAttribute('aria-label') || '');
            if (ph.includes('message') || al.includes('message')) {
              el.focus();
              return;
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
        /** Same crosshair as pre-click shot so both PNGs show where the mic was hit (vs blue recording bar). */
        await captureFollowUpScreenshotWithMarkers(
          page,
          [{ x: cx, y: cy, label: 'mic click point (reference)' }],
          'voice-after-mic-click',
          shotMeta
        );
      }
      await delay(Math.max(0, effectiveHoldMs - afterShotMs));
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

    await micEl.dispose().catch(() => {});
    await micHandle.dispose().catch(() => {});

    /** Do NOT send Escape here — it dismisses Instagram's voice recording UI before Send. */
    await delay(1200);
    const clickSend = clickSendAfterRecordingScript();
    let sent = false;
    for (let attempt = 0; attempt < 15; attempt++) {
      sent = await page.evaluate(clickSend).catch(() => false);
      if (sent) break;
      await delay(450);
    }
    if (!sent) return { ok: false, reason: 'voice_send_button_not_found' };

    if (logger) logger.log('Voice: send control clicked; waiting for thread to update…');
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
