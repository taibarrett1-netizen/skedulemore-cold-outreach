/**
 * Dismiss blocking Instagram Web modals — shared by the send worker (bot.js)
 * and the scrape worker (scraper.js).
 *
 * Call dismissInstagramPopups(page, logger) after every page.goto() to handle
 * whatever Instagram decides to throw up that day:
 *   - Cookie consent  ("Allow the use of cookies from Instagram on this browser?")
 *   - Account-switcher / profile "Continue" confirmation
 *   - "Turn on Notifications" / "Save your login" dialogs
 *   - "Review and Agree" / terms / privacy update dialogs
 *   - "See this post in the app" / comment upsell (close only, not Open Instagram)
 *
 * For /accounts/login with saved cookies, call activateInstagramSavedSessionFromLoginPage
 * (used by comment scraper after home load) to tap "Continue as @user" without a password.
 *
 * Session establishment diagnostics: set SCRAPER_DEBUG=1 or SCRAPER_SESSION_DEBUG=1 for
 * button samples, body snippets, and PNGs under logs/comment-scrape-debug/ (session_ensure_*).
 */

const path = require('path');
const fs = require('fs');

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function sessionEstablishmentDebugEnabled() {
  const v = String(process.env.SCRAPER_DEBUG || process.env.SCRAPER_SESSION_DEBUG || '')
    .trim()
    .toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
}

async function sessionEnsureDebugScreenshot(page, logger, jobId, suffix) {
  if (!sessionEstablishmentDebugEnabled()) return;
  try {
    const dir = path.join(__dirname, '..', 'logs', 'comment-scrape-debug');
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const safeJob = String(jobId || 'job').replace(/[^a-zA-Z0-9._-]+/g, '_').slice(0, 48);
    const safeSuffix = String(suffix).replace(/[^a-zA-Z0-9._-]+/g, '_').slice(0, 96);
    const out = path.join(dir, `${safeJob}_session_ensure_${safeSuffix}_${Date.now()}.png`);
    await page.screenshot({ path: out, type: 'png', fullPage: false });
    if (logger) logger.log('[instagram-modals] session-ensure debug PNG -> ' + out);
  } catch (e) {
    if (logger) logger.warn('[instagram-modals] session-ensure debug PNG failed: ' + (e.message || e));
  }
}

// ---------------------------------------------------------------------------
// Cookie consent
// ---------------------------------------------------------------------------

/**
 * Dismiss the "Allow the use of cookies from Instagram on this browser?" sheet.
 * Prefers "Allow all cookies"; falls back to "Decline optional cookies" so the
 * page unblocks either way.  Safe to call at any time — returns false quickly
 * if the popup isn't present.
 */
async function dismissInstagramCookieConsent(page) {
  for (let attempt = 0; attempt < 4; attempt++) {
    const clicked = await page.evaluate(() => {
      const roots = Array.from(document.querySelectorAll('[role="dialog"], [role="alertdialog"], div'));
      const targets = roots.filter((el) => {
        const t = (el.textContent || '').toLowerCase();
        return (
          t.includes('allow the use of cookies') ||
          t.includes('allow all cookies') ||
          t.includes('cookie') ||
          t.includes('die verwendung von cookies') ||
          t.includes('cookies durch instagram')
        );
      });
      for (const root of targets) {
        const clickables = Array.from(root.querySelectorAll('button, [role="button"], a, span'));
        const preferred =
          clickables.find((el) =>
            /allow all cookies|allow all|accept all|alle cookies erlauben|cookies erlauben/i.test(
              (el.textContent || '').trim()
            )
          ) ||
          clickables.find((el) =>
            /decline optional cookies|only allow essential|essential cookies|optionale cookies ablehnen|nur erforderliche cookies/i.test(
              (el.textContent || '').trim()
            )
          );
        if (preferred && preferred.offsetParent) {
          const btn = preferred.closest('[role="button"]') || preferred.closest('button') || preferred;
          btn.click();
          return true;
        }
      }
      return false;
    });
    if (!clicked) return false;
    await delay(900);
  }
  return true;
}

// ---------------------------------------------------------------------------
// Account-switcher / profile "Continue" confirmation
// ---------------------------------------------------------------------------

/**
 * Resolve the one-tap **Continue** target and cheap diagnostics (for logs / screenshots).
 * @returns {Promise<{url:string,found:boolean,cx:number,cy:number,label:string,buttonSamples:object[],bodySnippet:string,hasContinueContext:boolean}>}
 */
async function resolveContinueButtonDiagnostics(page, usernameHint) {
  const hint = usernameHint ? String(usernameHint).trim().replace(/^@/, '').toLowerCase() : '';
  return page.evaluate((h) => {
    function visible(el) {
      if (!el) return false;
      const r = el.getBoundingClientRect();
      return r.width > 2 && r.height > 2;
    }

    const bodyText = (document.body && document.body.innerText) || '';
    const bodyLower = bodyText.toLowerCase();
    const hasContinueContext =
      bodyLower.indexOf('use another profile') !== -1 ||
      bodyLower.indexOf('log in to another') !== -1 ||
      bodyLower.indexOf('create new account') !== -1 ||
      bodyLower.indexOf('continue as') !== -1 ||
      bodyLower.indexOf('agree and continue') !== -1 ||
      bodyLower.indexOf('continue to instagram') !== -1 ||
      (h &&
        bodyLower.indexOf(h) !== -1 &&
        /\bcontinue\b/.test(bodyLower) &&
        (bodyLower.indexOf('create new account') !== -1 ||
          bodyLower.indexOf('use another profile') !== -1 ||
          bodyLower.indexOf('meta') !== -1));

    function normalizeLabel(el) {
      return (el.textContent || '').replace(/\s+/g, ' ').trim();
    }

    function isPrimaryContinueLabel(t) {
      if (!t || t.length > 72) return false;
      if (/use another profile|log in to another profile|create new account/i.test(t)) return false;
      if (/^continue(\s+as\b.*)?$/i.test(t)) return true;
      if (/^agree and continue$/i.test(t)) return true;
      if (/^continue to instagram$/i.test(t)) return true;
      return false;
    }

    const buttonEls = Array.from(document.querySelectorAll('button')).filter(visible);
    const samples = buttonEls.slice(0, 20).map(function (b) {
      return {
        tag: b.tagName,
        text: normalizeLabel(b).slice(0, 100),
      };
    });

    const candidates = [];
    const roles = Array.from(
      document.querySelectorAll('div[role="button"], a[role="button"], [role="button"]:not(button)')
    ).filter(visible);
    for (let i = 0; i < buttonEls.length; i++) candidates.push(buttonEls[i]);
    for (let i = 0; i < roles.length; i++) candidates.push(roles[i]);

    let best = null;
    let bestScore = -1;
    for (let i = 0; i < candidates.length; i++) {
      const el = candidates[i];
      const t = normalizeLabel(el);
      if (!isPrimaryContinueLabel(t)) continue;
      let score = el.tagName === 'BUTTON' ? 40 : 10;
      const tl = t.toLowerCase();
      if (h && tl.indexOf(h) !== -1) score += 50;
      if (/^continue$/i.test(t)) score += 20;
      if (score > bestScore) {
        bestScore = score;
        best = el;
      }
    }

    var cx = 0;
    var cy = 0;
    var label = '';
    if (best) {
      const r = best.getBoundingClientRect();
      cx = r.left + r.width / 2;
      cy = r.top + r.height / 2;
      label = normalizeLabel(best).slice(0, 100);
    }

    return {
      url: location.href,
      found: !!best,
      cx: cx,
      cy: cy,
      label: label,
      buttonSamples: samples,
      bodySnippet: bodyText.slice(0, 1800).replace(/\s+/g, ' '),
      hasContinueContext: hasContinueContext,
    };
  }, hint);
}

/**
 * Clicks **Continue** on the saved-account / one-tap screen using a **rotating**
 * strategy (mouse center → touchscreen tap → pointer events → focus+Enter).
 * One strategy per call; pair with ensure-pool rounds so each retry differs.
 *
 * @param {object} [options] — { strategyIndex: number, debugJobId: string|null }
 */
async function clickInstagramProfileContinueIfPresent(page, logger, usernameHint, options) {
  const opts = options || {};
  const strategyIndex = opts.strategyIndex != null ? Number(opts.strategyIndex) : 0;
  const debugJobId = opts.debugJobId != null ? opts.debugJobId : null;

  const hint = usernameHint ? String(usernameHint).trim().replace(/^@/, '').toLowerCase() : '';
  const diag = await resolveContinueButtonDiagnostics(page, hint);

  if (sessionEstablishmentDebugEnabled() && logger) {
    logger.log(
      '[instagram-modals] Continue diag url=' +
        diag.url +
        ' hasContext=' +
        diag.hasContinueContext +
        ' found=' +
        diag.found +
        ' cx=' +
        diag.cx +
        ' cy=' +
        diag.cy +
        ' strategyIndex=' +
        (strategyIndex % 4)
    );
    logger.log('[instagram-modals] Continue label=' + JSON.stringify(diag.label));
    try {
      logger.log('[instagram-modals] Continue buttonSamples=' + JSON.stringify(diag.buttonSamples));
    } catch (_) {}
    const snip = diag.bodySnippet.length > 700 ? diag.bodySnippet.slice(0, 700) + '…' : diag.bodySnippet;
    logger.log('[instagram-modals] Continue bodySnippet=' + JSON.stringify(snip));
  }

  await sessionEnsureDebugScreenshot(page, logger, debugJobId, 'round' + strategyIndex + '-before-continue');

  if (!diag.hasContinueContext || !diag.found) {
    return false;
  }

  const strat = strategyIndex % 4;
  if (strat === 0) {
    await page.mouse.click(diag.cx, diag.cy, { delay: 90 });
    if (logger) logger.log('[instagram-modals] Continue action=strategy_mouse_center');
  } else if (strat === 1) {
    try {
      await page.touchscreen.tap(diag.cx, diag.cy);
      if (logger) logger.log('[instagram-modals] Continue action=strategy_touchscreen_tap');
    } catch (e) {
      await page.mouse.click(diag.cx, diag.cy, { delay: 90 });
      if (logger) logger.log('[instagram-modals] Continue action=strategy_touch_fallback_mouse (' + (e.message || e) + ')');
    }
  } else if (strat === 2) {
    await page.evaluate((h) => {
      function visible(el) {
        if (!el) return false;
        const r = el.getBoundingClientRect();
        return r.width > 2 && r.height > 2;
      }
      function normalizeLabel(el) {
        return (el.textContent || '').replace(/\s+/g, ' ').trim();
      }
      function isPrimaryContinueLabel(t) {
        if (!t || t.length > 72) return false;
        if (/use another profile|log in to another profile|create new account/i.test(t)) return false;
        if (/^continue(\s+as\b.*)?$/i.test(t)) return true;
        if (/^agree and continue$/i.test(t)) return true;
        if (/^continue to instagram$/i.test(t)) return true;
        return false;
      }
      const candidates = [];
      const buttonEls = Array.from(document.querySelectorAll('button')).filter(visible);
      const roles = Array.from(
        document.querySelectorAll('div[role="button"], a[role="button"], [role="button"]:not(button)')
      ).filter(visible);
      for (let i = 0; i < buttonEls.length; i++) candidates.push(buttonEls[i]);
      for (let i = 0; i < roles.length; i++) candidates.push(roles[i]);
      let best = null;
      let bestScore = -1;
      for (let i = 0; i < candidates.length; i++) {
        const el = candidates[i];
        const t = normalizeLabel(el);
        if (!isPrimaryContinueLabel(t)) continue;
        let score = el.tagName === 'BUTTON' ? 40 : 10;
        const tl = t.toLowerCase();
        if (h && tl.indexOf(h) !== -1) score += 50;
        if (/^continue$/i.test(t)) score += 20;
        if (score > bestScore) {
          bestScore = score;
          best = el;
        }
      }
      if (!best) return;
      const r = best.getBoundingClientRect();
      const x = r.left + r.width / 2;
      const y = r.top + r.height / 2;
      try {
        best.dispatchEvent(
          new PointerEvent('pointerdown', {
            bubbles: true,
            cancelable: true,
            clientX: x,
            clientY: y,
            pointerId: 1,
            pointerType: 'touch',
            isPrimary: true,
          })
        );
        best.dispatchEvent(
          new PointerEvent('pointerup', {
            bubbles: true,
            cancelable: true,
            clientX: x,
            clientY: y,
            pointerId: 1,
            pointerType: 'touch',
            isPrimary: true,
          })
        );
      } catch (_) {}
      best.click();
    }, hint);
    if (logger) logger.log('[instagram-modals] Continue action=strategy_pointer_dom_click');
  } else {
    const focused = await page.evaluate((h) => {
      function visible(el) {
        if (!el) return false;
        const r = el.getBoundingClientRect();
        return r.width > 2 && r.height > 2;
      }
      function normalizeLabel(el) {
        return (el.textContent || '').replace(/\s+/g, ' ').trim();
      }
      function isPrimaryContinueLabel(t) {
        if (!t || t.length > 72) return false;
        if (/use another profile|log in to another profile|create new account/i.test(t)) return false;
        if (/^continue(\s+as\b.*)?$/i.test(t)) return true;
        if (/^agree and continue$/i.test(t)) return true;
        if (/^continue to instagram$/i.test(t)) return true;
        return false;
      }
      const candidates = [];
      const buttonEls = Array.from(document.querySelectorAll('button')).filter(visible);
      const roles = Array.from(
        document.querySelectorAll('div[role="button"], a[role="button"], [role="button"]:not(button)')
      ).filter(visible);
      for (let i = 0; i < buttonEls.length; i++) candidates.push(buttonEls[i]);
      for (let i = 0; i < roles.length; i++) candidates.push(roles[i]);
      let best = null;
      let bestScore = -1;
      for (let i = 0; i < candidates.length; i++) {
        const el = candidates[i];
        const t = normalizeLabel(el);
        if (!isPrimaryContinueLabel(t)) continue;
        let score = el.tagName === 'BUTTON' ? 40 : 10;
        const tl = t.toLowerCase();
        if (h && tl.indexOf(h) !== -1) score += 50;
        if (/^continue$/i.test(t)) score += 20;
        if (score > bestScore) {
          bestScore = score;
          best = el;
        }
      }
      if (!best) return false;
      best.scrollIntoView({ block: 'center', inline: 'nearest' });
      best.focus();
      return true;
    }, hint);
    if (focused) {
      await delay(200);
      await page.keyboard.press('Enter');
      if (logger) logger.log('[instagram-modals] Continue action=strategy_focus_enter');
    } else if (logger) {
      logger.warn('[instagram-modals] Continue action=strategy_focus_enter skipped (no target)');
    }
  }

  try {
    await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 25000 });
  } catch (_) {
    await delay(2500);
  }
  await delay(1800);

  await sessionEnsureDebugScreenshot(page, logger, debugJobId, 'round' + strategyIndex + '-after-continue');

  return true;
}

async function dismissInstagramProfileContinue(page, logger, usernameHint) {
  return clickInstagramProfileContinueIfPresent(page, logger, usernameHint != null ? usernameHint : null, {
    strategyIndex: 0,
    debugJobId: null,
  });
}

/**
 * Comment / pool scraper: cookie consent → Continue as pool user → modals →
 * optional /accounts/login saved-tap. Repeats until the one-tap shell is gone
 * or max rounds (stale cookies still fail later on /accounts/login).
 */
async function ensurePoolScraperInstagramWebSession(page, logger, usernameHint, debugJobId) {
  const hint = (usernameHint || '').trim().replace(/^@/, '').toLowerCase();
  for (let round = 0; round < 5; round++) {
    if (logger) {
      logger.log('[instagram-modals] Pool scraper session ensure round ' + (round + 1) + '/5 url=' + page.url());
    }
    await sessionEnsureDebugScreenshot(page, logger, debugJobId, 'round' + round + '-loop-start');

    try {
      const cookieDismissed = await dismissInstagramCookieConsent(page);
      if (cookieDismissed && logger) {
        logger.log('[instagram-modals] Dismissed cookie consent popup');
        await delay(1200);
      }
    } catch (e) {
      if (logger) logger.log('[instagram-modals] cookie consent check error: ' + e.message);
    }

    await clickInstagramProfileContinueIfPresent(page, logger, hint || null, {
      strategyIndex: round,
      debugJobId: debugJobId != null ? debugJobId : null,
    });

    try {
      await dismissInstagramHomeModals(page, logger);
    } catch (e) {
      if (logger) logger.log('[instagram-modals] home modal check error: ' + e.message);
    }
    try {
      await dismissInstagramReviewDialogs(page, logger);
    } catch (e) {
      if (logger) logger.log('[instagram-modals] review dialog check error: ' + e.message);
    }
    try {
      await dismissInstagramAppWebUpsell(page, logger);
    } catch (e) {
      if (logger) logger.log('[instagram-modals] app web upsell check error: ' + e.message);
    }
    try {
      await activateInstagramSavedSessionFromLoginPage(page, logger, hint);
    } catch (e) {
      if (logger) logger.log('[instagram-modals] saved-session login tap error: ' + e.message);
    }

    const url = page.url() || '';
    if (url.indexOf('/accounts/login') !== -1) {
      await delay(1000);
      continue;
    }

    const stillOnOneTap = await page.evaluate((h) => {
      const b = ((document.body && document.body.innerText) || '').toLowerCase();
      if (!/\bcontinue\b/.test(b)) return false;
      if (!(b.indexOf('create new account') !== -1 || b.indexOf('use another profile') !== -1)) return false;
      if (h && b.indexOf(h) === -1) return false;
      return true;
    }, hint);

    if (!stillOnOneTap) {
      if (logger) logger.log('[instagram-modals] Pool scraper: left one-tap / login chooser shell');
      await sessionEnsureDebugScreenshot(page, logger, debugJobId, 'round' + round + '-success-exit');
      return;
    }
    if (logger) {
      logger.log(
        '[instagram-modals] Pool scraper: still on Continue screen after strategy ' +
          (round % 4) +
          ', retry ' +
          (round + 1) +
          '/5 url=' +
          page.url()
      );
    }
    await sessionEnsureDebugScreenshot(page, logger, debugJobId, 'round' + round + '-still-one-tap');
    await delay(1000);
  }
  await sessionEnsureDebugScreenshot(page, logger, debugJobId, 'exhausted-5-rounds');
}

// ---------------------------------------------------------------------------
// Notifications / "Save your login" dialogs
// ---------------------------------------------------------------------------

/**
 * After landing on instagram.com — dismiss "Turn on Notifications", "Save your
 * login", and similar home-page blocking modals.
 */
async function dismissInstagramHomeModals(page, logger) {
  for (let attempt = 0; attempt < 6; attempt++) {
    const clicked = await page.evaluate(() => {
      const isNotNow = (el) => /^not now$/i.test((el.textContent || '').replace(/\s+/g, ' ').trim());

      const dialogs = document.querySelectorAll('[role="dialog"], [role="alertdialog"]');
      for (let d = 0; d < dialogs.length; d++) {
        const root = dialogs[d];
        const txt = (root.textContent || '').toLowerCase();
        const relevant =
          txt.includes('turn on notifications') ||
          txt.includes('save your login') ||
          (txt.includes('notification') && txt.includes('know right away'));
        if (!relevant) continue;
        const clickables = Array.from(
          root.querySelectorAll('button, div[role="button"], span[role="button"], span, a')
        );
        const notNow = clickables.find((el) => {
          if (!el.offsetParent) return false;
          return isNotNow(el);
        });
        if (notNow) {
          const btn =
            notNow.closest('[role="button"]') ||
            notNow.closest('button') ||
            notNow.closest('a') ||
            notNow;
          btn.click();
          return 'not_now_dialog';
        }
      }

      // DM thread / mobile: overlay sometimes has no dialog role.
      const bodyLower = ((document.body && document.body.innerText) || '').toLowerCase();
      if (bodyLower.includes('turn on notifications') && bodyLower.includes('not now')) {
        const candidates = Array.from(
          document.querySelectorAll('button, [role="button"], div[role="button"], span, a')
        );
        const hit = candidates.find((el) => el.offsetParent && isNotNow(el));
        if (hit) {
          const btn = hit.closest('button, [role="button"], a') || hit;
          btn.click();
          return 'not_now_global';
        }
      }
      return false;
    });
    if (clicked) {
      if (logger) logger.log('[instagram-modals] Dismissed notification/login modal: ' + clicked);
      await delay(800);
      continue;
    }
    break;
  }
}

// ---------------------------------------------------------------------------
// "Open app" / web upsell (mobile web often blocks comments behind this sheet)
// ---------------------------------------------------------------------------

/**
 * Dismiss "See this post in the app" / "Use the app to view all comments" sheets.
 * Prefers close/dismiss controls — never clicks "Open Instagram" (would leave web).
 */
async function dismissInstagramAppWebUpsell(page, logger) {
  for (let attempt = 0; attempt < 5; attempt++) {
    const action = await page.evaluate(() => {
      const body = ((document.body && document.body.innerText) || '').toLowerCase();
      const upsell =
        body.includes('see this post in the app') ||
        body.includes('use the app to view all comments') ||
        (body.includes('open instagram') &&
          (body.includes('sign up') || body.includes('see this post')));
      if (!upsell) return null;

      function rectVisible(el) {
        if (!el) return false;
        const r = el.getBoundingClientRect();
        return r.width > 4 && r.height > 4;
      }

      const all = Array.from(
        document.querySelectorAll('[aria-label], button, [role="button"], a, div[role="button"]')
      );
      for (let i = 0; i < all.length; i++) {
        const el = all[i];
        if (!rectVisible(el)) continue;
        const al = (el.getAttribute('aria-label') || '').toLowerCase();
        if (
          /\bclose\b/.test(al) ||
          /\bdismiss\b/.test(al) ||
          al === 'back' ||
          al.includes('close') ||
          al.includes('schlie') /* de */
        ) {
          (el.closest('[role="button"]') || el.closest('button') || el).click();
          return 'aria_label_close';
        }
      }

      const dialogs = Array.from(document.querySelectorAll('[role="dialog"], [role="presentation"]'));
      for (let d = 0; d < dialogs.length; d++) {
        const root = dialogs[d];
        const svgs = root.querySelectorAll('svg');
        for (let s = 0; s < svgs.length; s++) {
          const btn = svgs[s].closest('button, [role="button"], a, div[role="button"]');
          if (!btn || !rectVisible(btn)) continue;
          const r = btn.getBoundingClientRect();
          if (r.top < 140 && r.right > window.innerWidth * 0.5) {
            btn.click();
            return 'dialog_corner_svg';
          }
        }
      }

      return 'try_escape';
    });

    if (action === 'try_escape') {
      await page.keyboard.press('Escape').catch(() => {});
      await delay(700);
      continue;
    }
    if (action) {
      if (logger) logger.log('[instagram-modals] Dismissed app/web upsell: ' + action);
      await delay(1100);
      continue;
    }
    break;
  }
}

// ---------------------------------------------------------------------------
// Saved session on /accounts/login (tap profile / "Continue as" — no password)
// ---------------------------------------------------------------------------

/**
 * When cookies exist but IG shows the login chooser, click through to the known
 * pool username without entering a password.
 */
async function activateInstagramSavedSessionFromLoginPage(page, logger, usernameHint) {
  const url = page.url() || '';
  if (!url.includes('instagram.com') || !url.includes('/accounts/login')) return false;

  const hint = String(usernameHint || '')
    .trim()
    .replace(/^@/, '')
    .toLowerCase();
  if (!hint) return false;

  const clicked = await page.evaluate((un) => {
    function rectVisible(el) {
      if (!el) return false;
      const r = el.getBoundingClientRect();
      return r.width > 4 && r.height > 4;
    }

    const candidates = Array.from(
      document.querySelectorAll('a[href], button, [role="button"], div[role="button"]')
    );

    for (let i = 0; i < candidates.length; i++) {
      const el = candidates[i];
      if (!rectVisible(el)) continue;
      const t = (el.textContent || '').replace(/\s+/g, ' ').trim();
      const low = t.toLowerCase();
      if (/create new account|sign up for instagram|sign up with phone/i.test(low)) continue;
      if (/^log in to another profile$/i.test(low) || /^use another profile$/i.test(low)) continue;

      if (/^continue as\b/i.test(t)) {
        if (!un || low.includes(un)) {
          el.click();
          return 'continue_as';
        }
      }
      if (un && low === un && t.length <= 40) {
        el.click();
        return 'username_tile';
      }
    }

    for (let i = 0; i < candidates.length; i++) {
      const el = candidates[i];
      if (el.tagName !== 'A') continue;
      const href = (el.getAttribute('href') || '').toLowerCase();
      if (!href) continue;
      const profilePath = '/' + un + '/';
      const looksLikeProfile =
        href.includes(profilePath) ||
        href.endsWith('/' + un) ||
        href.includes('/' + un + '?');
      if (!looksLikeProfile) continue;
      if (href.includes('/accounts/signup') || href.includes('/accounts/emailsignup')) continue;
      if (rectVisible(el)) {
        el.click();
        return 'profile_href';
      }
    }

    return false;
  }, hint);

  if (clicked && logger) {
    logger.log('[instagram-modals] Activated saved session from login page: ' + clicked + ' (@' + hint + ')');
  }
  if (clicked) {
    try {
      await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 12000 });
    } catch (_) {
      await delay(2000);
    }
  }
  return Boolean(clicked);
}

// ---------------------------------------------------------------------------
// Review / Terms / Privacy update dialogs
// ---------------------------------------------------------------------------

/**
 * Handle "Review and Agree" / "Updates to our Terms" / "Changes to how we
 * manage data" dialogs.  Clicks Agree / Next / OK to unblock the page.
 */
async function dismissInstagramReviewDialogs(page, logger) {
  for (let i = 0; i < 3; i++) {
    const handled = await page.evaluate(() => {
      const bodyText = (document.body && document.body.innerText) || '';
      if (
        !/review and agree/i.test(bodyText) &&
        !/changes to how we manage data/i.test(bodyText) &&
        !/updates to our terms/i.test(bodyText)
      ) {
        return false;
      }
      const labels = ['Agree to Terms', 'Agree', 'Next', 'OK', 'Accept', 'Continue'];
      const buttons = Array.from(
        document.querySelectorAll('button, div[role="button"], [role="button"]')
      );
      for (const label of labels) {
        const btn = buttons.find(
          (el) => (el.textContent || '').trim().toLowerCase() === label.toLowerCase()
        );
        if (btn && btn.offsetParent) {
          btn.click();
          return true;
        }
      }
      // Fallback: primary blue button inside any dialog.
      const dialogs = Array.from(document.querySelectorAll('[role="dialog"]'));
      for (const d of dialogs) {
        const primary = Array.from(
          d.querySelectorAll('button, div[role="button"], [role="button"]')
        ).find((el) => {
          const bg = (window.getComputedStyle(el).backgroundColor || '');
          return /rgb\(0,\s*149,\s*246\)/.test(bg) || /rgb\(0,\s*55,\s*107\)/.test(bg);
        });
        if (primary && primary.offsetParent) {
          primary.click();
          return true;
        }
      }
      return false;
    });
    if (!handled) break;
    if (logger) logger.log('[instagram-modals] Dismissed Review/Terms dialog (' + (i + 1) + ')');
    await delay(1000);
  }
}

// ---------------------------------------------------------------------------
// Composite — call this after every page.goto()
// ---------------------------------------------------------------------------

/**
 * One-stop dismissal for all known Instagram blocking popups.  Safe to call
 * after any navigation — returns quickly if nothing needs dismissing.
 *
 * Order matters: cookies first (can obscure everything else), then
 * one-tap **Continue** (logs into saved session), then notifications, terms, app upsell.
 */
async function dismissInstagramPopups(page, logger) {
  try {
    const cookieDismissed = await dismissInstagramCookieConsent(page);
    if (cookieDismissed && logger) {
      logger.log('[instagram-modals] Dismissed cookie consent popup');
      // Give the page a moment to re-render after the cookie sheet closes before
      // checking for the account-switcher "Continue" button underneath.
      await delay(1200);
    }
  } catch (e) {
    if (logger) logger.log('[instagram-modals] cookie consent check error: ' + e.message);
  }

  try {
    const continueClicked = await dismissInstagramProfileContinue(page, logger, null);
    if (continueClicked && logger) {
      logger.log('[instagram-modals] Activated session via Continue (generic popup pass)');
    }
  } catch (e) {
    if (logger) logger.log('[instagram-modals] profile continue check error: ' + e.message);
  }

  try {
    await dismissInstagramHomeModals(page, logger);
  } catch (e) {
    if (logger) logger.log('[instagram-modals] home modal check error: ' + e.message);
  }

  try {
    await dismissInstagramReviewDialogs(page, logger);
  } catch (e) {
    if (logger) logger.log('[instagram-modals] review dialog check error: ' + e.message);
  }

  try {
    await dismissInstagramAppWebUpsell(page, logger);
  } catch (e) {
    if (logger) logger.log('[instagram-modals] app web upsell check error: ' + e.message);
  }
}

/** Close sticker picker / GIF / emoji popovers that steal clicks from the mic. */
async function closeDmComposerOverlays(page) {
  for (let i = 0; i < 4; i++) {
    await page.keyboard.press('Escape');
    await delay(180);
  }
}

/**
 * True when mobile web shows the password re-auth screen (saved username + password field + Log in),
 * i.e. cookies no longer keep the session — user must reconnect in the dashboard.
 */
async function detectInstagramPasswordReauthScreen(page) {
  try {
    const u = page.url() || '';
    if (!/\/accounts\/login/i.test(u)) return false;
    return await page.evaluate(() => {
      const pw = document.querySelector('input[type="password"], input[name="pass"], input[name="password"]');
      if (!pw || !pw.offsetParent) return false;
      const body = ((document.body && document.body.innerText) || '').toLowerCase();
      return /log in|anmelden|accedi|iniciar sesión/.test(body);
    });
  } catch (_) {
    return false;
  }
}

module.exports = {
  dismissInstagramCookieConsent,
  resolveContinueButtonDiagnostics,
  clickInstagramProfileContinueIfPresent,
  dismissInstagramProfileContinue,
  dismissInstagramHomeModals,
  dismissInstagramReviewDialogs,
  dismissInstagramAppWebUpsell,
  activateInstagramSavedSessionFromLoginPage,
  ensurePoolScraperInstagramWebSession,
  dismissInstagramPopups,
  closeDmComposerOverlays,
  detectInstagramPasswordReauthScreen,
  delay,
};
