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
 */

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
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
 * Dismiss the account-switcher overlay that Instagram shows on the home page
 * or when landing on a profile — the one with a "Continue" (or "Continue as X")
 * button next to "Log in to another profile" / "Create new account".
 * Also handles "Continue" buttons on general consent/agree interstitials.
 */
async function dismissInstagramProfileContinue(page) {
  for (let attempt = 0; attempt < 3; attempt++) {
    const clicked = await page.evaluate(() => {
      function visible(el) {
        return !!(el && el.offsetParent);
      }

      // Detect the account-switcher context: page or dialog contains "Continue" +
      // one of the surrounding signals.
      const bodyText = ((document.body && document.body.innerText) || '').toLowerCase();
      const hasContinueContext =
        bodyText.includes('log in to another') ||
        bodyText.includes('create new account') ||
        bodyText.includes('continue as') ||
        // Generic consent/interstitial "Continue" button
        bodyText.includes('agree and continue') ||
        bodyText.includes('continue to instagram');

      if (!hasContinueContext) return false;

      const candidates = Array.from(
        document.querySelectorAll('button, [role="button"], div[role="button"], a')
      );
      // Prefer the primary blue "Continue" / "Continue as …" button.
      const hit =
        candidates.find(
          (el) =>
            visible(el) &&
            /^continue(\s+as\s+\S+)?$/i.test((el.textContent || '').replace(/\s+/g, ' ').trim())
        ) ||
        candidates.find(
          (el) =>
            visible(el) &&
            /agree and continue/i.test((el.textContent || '').trim())
        );

      if (hit) {
        hit.click();
        return true;
      }
      return false;
    });
    if (!clicked) return false;
    await delay(800);
  }
  return true;
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
 * account-switcher "Continue", then notifications, then terms.
 */
async function dismissInstagramPopups(page, logger) {
  try {
    const cookieDismissed = await dismissInstagramCookieConsent(page);
    if (cookieDismissed && logger) {
      logger.log('[instagram-modals] Dismissed cookie consent popup');
    }
  } catch (e) {
    if (logger) logger.log('[instagram-modals] cookie consent check error: ' + e.message);
  }

  try {
    const continueDismissed = await dismissInstagramProfileContinue(page);
    if (continueDismissed && logger) {
      logger.log('[instagram-modals] Dismissed profile Continue/account-switcher popup');
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
}

/** Close sticker picker / GIF / emoji popovers that steal clicks from the mic. */
async function closeDmComposerOverlays(page) {
  for (let i = 0; i < 4; i++) {
    await page.keyboard.press('Escape');
    await delay(180);
  }
}

module.exports = {
  dismissInstagramCookieConsent,
  dismissInstagramProfileContinue,
  dismissInstagramHomeModals,
  dismissInstagramReviewDialogs,
  dismissInstagramPopups,
  closeDmComposerOverlays,
  delay,
};
