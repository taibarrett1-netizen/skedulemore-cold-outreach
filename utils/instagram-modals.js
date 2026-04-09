/**
 * Dismiss blocking Instagram Web modals (home feed, DM composer overlays).
 */
function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * After landing on instagram.com — dismiss "Turn on Notifications" and similar.
 * Without this, the feed is blocked and later navigation can behave oddly.
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
        const clickables = Array.from(root.querySelectorAll('button, div[role="button"], span[role="button"], span, a'));
        const notNow = clickables.find((el) => {
          if (!el.offsetParent) return false;
          return isNotNow(el);
        });
        if (notNow) {
          const btn = notNow.closest('[role="button"]') || notNow.closest('button') || notNow.closest('a') || notNow;
          btn.click();
          return 'not_now_dialog';
        }
      }

      // DM thread / mobile: overlay sometimes has no dialog role — still shows in body text.
      const bodyLower = ((document.body && document.body.innerText) || '').toLowerCase();
      if (bodyLower.includes('turn on notifications') && bodyLower.includes('not now')) {
        const candidates = Array.from(document.querySelectorAll('button, [role="button"], div[role="button"], span, a'));
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
      if (logger) logger.log('Dismissed Instagram modal (notifications or similar): ' + clicked);
      await delay(800);
      continue;
    }
    break;
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
  dismissInstagramHomeModals,
  closeDmComposerOverlays,
  delay,
};
