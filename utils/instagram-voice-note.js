/**
 * Instagram Web DM voice note: find mic control, record, send.
 * Desktop viewport is required (see applyDesktopEmulation).
 *
 * Chromium still requests microphone access for WebRTC/getUserMedia; on a VPS the
 * "mic" is typically PulseAudio (virtual sink). Grant permission in the browser so
 * no OS dialog blocks automation (Safari dialogs are not in the page DOM).
 */

/** Puppeteer removed `page.waitForTimeout`; use this instead. */
function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

    const matchesMic = (el) => {
      if (!rectVisible(el)) return false;
      const t = textHints(el);
      if (!t) return false;
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

    const hit = candidates.find(matchesMic);
    if (hit) {
      if (hit.tagName === 'SVG' || (hit.tagName && hit.tagName.toLowerCase() === 'svg')) {
        const clickable = hit.closest('button, [role="button"], a') || hit.parentElement;
        return clickable && rectVisible(clickable) ? clickable : hit;
      }
      return hit;
    }

    /** Desktop Web: mic is often icon-only (no aria-label). Right cluster is [mic][gallery][heart]. */
    function findMicByComposerLayout() {
      const inputs = Array.from(
        document.querySelectorAll('textarea[placeholder], [contenteditable="true"], [role="textbox"]')
      );
      const compose = inputs.find((el) => {
        const ph = lower(el.getAttribute('placeholder') || '');
        const al = lower(el.getAttribute('aria-label') || '');
        return ph.includes('message') || al.includes('message');
      });
      const bottomY = window.innerHeight - 140;
      const row = Array.from(document.querySelectorAll('button, div[role="button"], span[role="button"]'))
        .filter((el) => {
          if (!el.querySelector || !el.querySelector('svg')) return false;
          if (!rectVisible(el)) return false;
          const r = el.getBoundingClientRect();
          if (r.top < bottomY) return false;
          if (r.width < 8 || r.height < 8) return false;
          return true;
        })
        .sort((a, b) => a.getBoundingClientRect().left - b.getBoundingClientRect().left);

      if (compose) {
        const cr = compose.getBoundingClientRect();
        const rightStrip = row.filter((el) => {
          const r = el.getBoundingClientRect();
          return r.left >= cr.left + Math.min(40, cr.width * 0.15);
        });
        if (rightStrip.length >= 3) {
          const trio = rightStrip.slice(-3);
          const micCandidate = trio[0];
          if (micCandidate && rectVisible(micCandidate)) return micCandidate;
        }
      }

      if (row.length >= 3) {
        const trio = row.slice(-3);
        const micCandidate = trio[0];
        if (micCandidate && rectVisible(micCandidate)) return micCandidate;
      }
      return null;
    }

    return findMicByComposerLayout();
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
    const clickables = Array.from(
      document.querySelectorAll('button, div[role="button"], span[role="button"], a[role="button"]')
    );
    const send = clickables.find((el) => {
      if (!visible(el)) return false;
      const label = lower(el.getAttribute('aria-label'));
      const title = lower(el.getAttribute('title'));
      const txt = lower((el.textContent || '').trim());
      if (label.includes('send') || title.includes('send') || txt === 'send') return true;
      if (label.includes('send') && (label.includes('voice') || label.includes('message'))) return true;
      return false;
    });
    if (send) {
      send.click();
      return true;
    }
    const bottom = window.innerHeight - 160;
    const planes = clickables
      .filter((el) => {
        if (!visible(el)) return false;
        if (!el.querySelector('svg')) return false;
        const r = el.getBoundingClientRect();
        return r.top >= bottom;
      })
      .sort((a, b) => b.getBoundingClientRect().right - a.getBoundingClientRect().right);
    if (planes.length && planes[0]) {
      planes[0].click();
      return true;
    }
    return false;
  };
}

/**
 * Desktop: tap mic → record for holdMs → tap Send (paper plane).
 * Mobile web: press-and-hold on mic for holdMs, then Send.
 */
async function sendVoiceNoteInThread(page, opts = {}) {
  const { holdMs = 7000, logger = null, beforeSendClick = null } = opts;

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

  if (desktopFlow) {
    if (logger) logger.log(`Voice (desktop): click mic, record ${Math.round(holdMs)} ms, then send`);
    await page.mouse.click(cx, cy, { delay: 40 });
    await delay(holdMs);
  } else {
    if (logger) logger.log(`Voice (press-hold): ${Math.round(holdMs)} ms`);
    await page.mouse.move(cx, cy);
    await page.mouse.down();
    await delay(holdMs);
    await page.mouse.up();
  }

  await micEl.dispose().catch(() => {});
  await micHandle.dispose().catch(() => {});

  await delay(800);
  if (typeof beforeSendClick === 'function') {
    try {
      await beforeSendClick(page);
    } catch (e) {
      if (logger) logger.warn(`[follow-up] beforeSendClick hook: ${e.message}`);
    }
  }
  const clickSend = clickSendAfterRecordingScript();
  let sent = await page.evaluate(clickSend).catch(() => false);
  if (!sent) {
    await delay(600);
    sent = await page.evaluate(clickSend).catch(() => false);
  }
  if (!sent) return { ok: false, reason: 'voice_send_button_not_found' };

  await delay(1200);
  return { ok: true };
}

module.exports = { sendVoiceNoteInThread, prepareVoiceNoteUi, grantMicrophoneForInstagram };
