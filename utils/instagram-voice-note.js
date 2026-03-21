/**
 * Instagram Web DM voice note: find mic control, record, send.
 * Desktop viewport is required (see applyDesktopEmulation).
 *
 * Chromium still requests microphone access for WebRTC/getUserMedia; on a VPS the
 * "mic" is typically PulseAudio (virtual sink). Grant permission in the browser so
 * no OS dialog blocks automation (Safari dialogs are not in the page DOM).
 */

const { closeDmComposerOverlays } = require('./instagram-modals');
const { captureFollowUpScreenshot, isFollowUpScreenshotsEnabled } = require('./follow-up-screenshots');

/** When not `false`, wait for thread DOM to change after Send (audio/list rows). Reduces false "sent ok". */
const VOICE_NOTE_STRICT_VERIFY = process.env.VOICE_NOTE_STRICT_VERIFY !== 'false';

/** Puppeteer removed `page.waitForTimeout`; use this instead. */
function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Rough metrics inside the main thread area (before/after voice send).
 * Instagram DOM changes often; we look for list growth or new <audio>.
 */
function threadDomMetricsScript() {
  return function collectThreadDomMetrics() {
    const root =
      document.querySelector('section[role="main"]') ||
      document.querySelector('[role="main"]') ||
      document.querySelector('main') ||
      document.body;
    const audios = root.querySelectorAll('audio');
    const listItems = root.querySelectorAll('[role="listitem"]');
    const rows = root.querySelectorAll('[role="row"]');
    return {
      audio: audios.length,
      listItems: listItems.length,
      rows: rows.length,
    };
  };
}

/**
 * After clicking Send, poll until we see a new message row / list item / audio vs snapshot.
 */
async function waitForVoiceDeliveredInThread(page, before, opts = {}) {
  const { timeoutMs = 18000, pollMs = 450, logger = null } = opts;
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const after = await page.evaluate(threadDomMetricsScript()).catch(() => null);
    if (after) {
      const gained =
        after.audio > before.audio ||
        after.listItems > before.listItems ||
        after.rows > before.rows;
      if (gained) {
        if (logger) {
          logger.log(
            `Voice verify: thread DOM changed (audio ${before.audio}→${after.audio}, listItems ${before.listItems}→${after.listItems}, rows ${before.rows}→${after.rows})`
          );
        }
        return true;
      }
    }
    await delay(pollMs);
  }
  if (logger) {
    logger.warn(
      `Voice verify: no thread change within ${timeoutMs}ms (audio ${before.audio}, listItems ${before.listItems}, rows ${before.rows})`
    );
  }
  return false;
}

/** Best-effort: recording UI often shows a mm:ss timer. */
function recordingUiHintScript() {
  return function recordingUiHint() {
    const t = document.body.innerText || '';
    const m = t.match(/\b(\d{1,2}:\d{2})\b/);
    return {
      hasTimerLike: !!m,
      timerSample: m ? m[1] : '',
      composerLower: (t || '').toLowerCase().includes('message'),
    };
  };
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
    holdMs = 7000,
    logger = null,
    correlationId = '',
    strictVerify = VOICE_NOTE_STRICT_VERIFY,
  } = opts;
  const shotMeta = { correlationId, logger };

  await closeDmComposerOverlays(page);

  const metricsBefore = await page.evaluate(threadDomMetricsScript()).catch(() => ({
    audio: 0,
    listItems: 0,
    rows: 0,
  }));
  if (logger) {
    logger.log(
      `Voice: thread snapshot before mic (audio=${metricsBefore.audio}, listItems=${metricsBefore.listItems}, rows=${metricsBefore.rows})`
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
  if (desktopFlow) {
    if (logger) logger.log(`Voice (desktop): click mic, wait ${Math.round(holdMs)} ms, then send`);
    /** Native DOM click on the resolved mic node (often more reliable than synthetic coords for React). */
    try {
      await micEl.click({ delay: 50 });
    } catch {
      await page.mouse.click(cx, cy, { delay: 40 });
    }
    await delay(afterShotMs);
    if (isFollowUpScreenshotsEnabled()) {
      await captureFollowUpScreenshot(page, 'voice-after-mic-click', shotMeta);
    }
    const recHint = await page.evaluate(recordingUiHintScript()).catch(() => ({}));
    if (logger) {
      logger.log(
        `Voice: after mic click hint timerLike=${recHint.hasTimerLike || false} sample=${recHint.timerSample || '-'}`
      );
    }
    await delay(Math.max(0, holdMs - afterShotMs));
  } else {
    if (logger) logger.log(`Voice (mobile web): press-and-hold ${Math.round(holdMs)} ms`);
    await page.mouse.move(cx, cy);
    await page.mouse.down();
    await delay(holdMs);
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
}

module.exports = {
  sendVoiceNoteInThread,
  prepareVoiceNoteUi,
  grantMicrophoneForInstagram,
  VOICE_NOTE_STRICT_VERIFY,
};
