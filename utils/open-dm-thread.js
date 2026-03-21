/**
 * Open Instagram Web DM thread to a user (same flow as cold DM sendDMOnce navigation).
 * @returns {Promise<{ ok: true } | { ok: false, reason: string, pageSnippet?: string }>}
 */
const logger = require('./logger');

function delay(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function humanDelay() {
  await delay(500 + Math.floor(Math.random() * 1500));
}

async function navigateToDmThread(page, u) {
  await page.goto('https://www.instagram.com/direct/new/', { waitUntil: 'networkidle2', timeout: 20000 });
  await humanDelay();

  for (let i = 0; i < 3; i++) {
    const dismissed = await page.evaluate(function () {
      const dialogs = document.querySelectorAll('[role="dialog"], [role="alertdialog"]');
      for (let d = 0; d < dialogs.length; d++) {
        const txt = (dialogs[d].textContent || '').toLowerCase();
        if (txt.indexOf('save your login') !== -1 || txt.indexOf('not now') !== -1 || txt.indexOf('turn on notifications') !== -1) {
          const notNow = Array.from(dialogs[d].querySelectorAll('span, button, div[role="button"]')).find(function (el) {
            return (el.textContent || '').trim().toLowerCase() === 'not now';
          });
          if (notNow) {
            const btn = notNow.closest('[role="button"]') || notNow.closest('button') || notNow;
            if (btn) {
              btn.click();
              return true;
            }
          }
        }
      }
      return false;
    });
    if (dismissed) {
      logger.log('Dismissed direct/new prompt');
      await delay(1500);
    } else {
      break;
    }
  }

  await page
    .waitForFunction(
      () => {
        const els = document.querySelectorAll('input, textarea, [contenteditable="true"]');
        return Array.from(els).some((el) => {
          try {
            if (!el || el.disabled) return false;
            return (el.getClientRects && el.getClientRects().length > 0) || el.offsetParent !== null;
          } catch {
            return false;
          }
        });
      },
      { timeout: 8000 }
    )
    .catch(() => {});

  const searchHandle = await page.evaluateHandle(() => {
    const normalize = (s) => (s || '').toString().toLowerCase();
    const candidates = Array.from(document.querySelectorAll('input, textarea, [contenteditable="true"]')).filter((el) => {
      try {
        if (!el || el.disabled) return false;
        if (el.type === 'hidden') return false;
        if (el.getClientRects && el.getClientRects().length > 0) return true;
        if (el.offsetParent !== null) return true;
        return false;
      } catch {
        return false;
      }
    });

    const findWithHints = (predicates) => {
      for (const pred of predicates) {
        const hit = candidates.find((el) => pred(el));
        if (hit) return hit;
      }
      return null;
    };

    const searchOrTo = (el) => {
      const ph = normalize(el.placeholder);
      const aria = normalize(el.getAttribute && el.getAttribute('aria-label'));
      return ph.includes('search') || ph.includes('to:') || aria.includes('search') || aria.includes('to:');
    };

    const comboboxRole = (el) => {
      const role = normalize(el.getAttribute && el.getAttribute('role'));
      return role === 'combobox' || role === 'textbox';
    };

    const textInput = (el) => {
      if (!('tagName' in el)) return false;
      if (el.tagName === 'INPUT') return !el.type || el.type === 'text';
      if (el.tagName === 'TEXTAREA') return true;
      return !!el.isContentEditable;
    };

    const hit =
      findWithHints([searchOrTo, comboboxRole]) ||
      findWithHints([textInput]) ||
      candidates[0] ||
      null;

    return hit;
  });

  const searchEl = searchHandle.asElement();
  if (!searchEl) {
    await searchHandle.dispose().catch(() => {});
    return { ok: false, reason: 'no_compose', pageSnippet: 'Search input not found on direct/new' };
  }

  const searchMeta = await page.evaluate((el) => ({ tag: el.tagName, type: el.type || '', isCE: !!el.isContentEditable }), searchEl).catch(() => ({}));
  await searchEl.click({ delay: 50 }).catch(() => {});

  if (searchMeta.tag === 'INPUT' || searchMeta.tag === 'TEXTAREA') {
    await searchEl.type(u, { delay: 90 });
  } else {
    await delay(100);
    await page.keyboard.type(u, { delay: 90 });
  }

  await searchEl.dispose();
  await searchHandle.dispose();
  await delay(2800);

  const userClicked = await page.evaluate((username) => {
    const needle = username.toLowerCase().replace(/^@/, '');
    const buttons = Array.from(document.querySelectorAll('div[role="button"]'));
    const userBtn = buttons.find((b) => {
      const t = (b.textContent || '').toLowerCase();
      return t.includes(needle) && !t.includes('more accounts');
    });
    if (userBtn) {
      userBtn.click();
      return true;
    }
    if (buttons.length) buttons[0].click();
    return false;
  }, u);
  if (!userClicked) {
    const { hint, pageSnippet, searchPreview } = await page
      .evaluate(() => {
        const body = document.body && document.body.innerText ? document.body.innerText : '';
        const lower = body.toLowerCase();
        let hint = 'user_not_found';
        if (lower.includes('this account is private') || lower.includes('account is private') || lower.includes('private account')) hint = 'account_private';
        else if (lower.includes("couldn't find") || lower.includes('could not find') || lower.includes('no results') || lower.includes('no users found')) hint = 'user_not_found';
        else if (lower.includes('try again later') || lower.includes('too many')) hint = 'rate_limited';
        const snippet = body.replace(/\s+/g, ' ').trim().slice(0, 120);
        const buttons = Array.from(document.querySelectorAll('div[role="button"]')).filter((b) => !(b.textContent || '').toLowerCase().includes('more accounts'));
        const preview = buttons.slice(0, 4).map((b) => (b.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 40)).filter(Boolean);
        return { hint, pageSnippet: snippet || '(empty)', searchPreview: preview.length ? preview.join(' | ') : '' };
      })
      .catch(() => ({ hint: 'user_not_found', pageSnippet: '(unable to read page)', searchPreview: '' }));
    const extra = searchPreview ? ' First results: ' + searchPreview : '';
    return { ok: false, reason: hint, pageSnippet: (pageSnippet || '') + extra };
  }
  await delay(1500);

  const openedThread = await page.evaluate(() => {
    const targets = ['button', 'div[role="button"]', 'a', 'span[role="button"]'];
    const candidates = [];
    for (const sel of targets) {
      document.querySelectorAll(sel).forEach((el) => {
        if (el.offsetParent !== null && el.offsetWidth > 0 && el.offsetHeight > 0) candidates.push(el);
      });
    }
    const needle = (t) => t.toLowerCase().replace(/\s+/g, ' ').trim();
    const labels = ['send message', 'message', 'next', 'chat', 'send a message', 'start a chat'];
    for (const label of labels) {
      const btn = candidates.find((el) => {
        const t = needle(el.textContent || '');
        return t === label || (t.includes('send') && t.includes('message')) || t === 'next' || t === 'chat';
      });
      if (btn) {
        btn.click();
        return true;
      }
    }
    for (const label of labels) {
      const btn = candidates.find((el) => needle(el.textContent || '').includes(label));
      if (btn) {
        btn.click();
        return true;
      }
    }
    return false;
  });
  if (openedThread) await delay(2500);
  await delay(2000);

  try {
    await page.waitForFunction(
      () => !window.location.pathname.includes('/direct/new') && window.location.pathname.includes('/direct/'),
      { timeout: 8000 }
    );
  } catch (e) {
    if (page.url().includes('/direct/new')) {
      await page.evaluate(() => {
        const clickables = Array.from(document.querySelectorAll('button, div[role="button"], a'));
        const nextOrChat = clickables.find((el) => {
          const t = (el.textContent || '').toLowerCase().trim();
          return t === 'next' || t === 'chat' || (t.includes('send') && t.includes('message'));
        });
        if (nextOrChat && nextOrChat.offsetParent) nextOrChat.click();
      });
      await delay(3000);
    }
  }
  await delay(2000);

  const composeSelector = 'textarea, div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"], [role="textbox"]';
  try {
    await page.waitForSelector(composeSelector, { timeout: 20000 });
  } catch (e) {
    const bodySnippet = await page
      .evaluate(() => (document.body && document.body.innerText ? document.body.innerText : '').slice(0, 400))
      .catch(() => '');
    const lower = bodySnippet.toLowerCase();
    let reason = 'no_compose';
    if (lower.includes('this account is private') || lower.includes('account is private')) reason = 'account_private';
    else if (lower.includes("can't message") || lower.includes("can't send") || lower.includes('message request')) reason = 'messages_restricted';
    return { ok: false, reason, pageSnippet: bodySnippet.replace(/\s+/g, ' ').slice(0, 120) };
  }

  return { ok: true };
}

/**
 * Send one plain text message in the current thread (compose must be visible).
 */
async function sendPlainTextInThread(page, text) {
  const msg = String(text || '').trim();
  if (!msg) return { ok: false, reason: 'empty_message' };

  const composeEl = await page.evaluateHandle(() => {
    const byPlaceholder = (el) => {
      const p = (el.getAttribute && el.getAttribute('placeholder')) || '';
      const a = (el.getAttribute && el.getAttribute('aria-label')) || '';
      const t = (p + ' ' + a).toLowerCase();
      return t.includes('message') || t.includes('add a message') || t.includes('write a message');
    };
    const all = document.querySelectorAll('textarea, div[contenteditable="true"], p[contenteditable="true"], [contenteditable="true"], [role="textbox"]');
    for (const el of all) {
      if (el.offsetParent === null) continue;
      if (byPlaceholder(el)) return el;
    }
    for (const el of all) {
      if (el.offsetParent !== null) return el;
    }
    return null;
  });
  const compose = composeEl.asElement();
  if (!compose) {
    await composeEl.dispose();
    return { ok: false, reason: 'no_compose' };
  }
  await delay(400);
  await compose.click();
  await compose.type(msg, { delay: 55 + Math.floor(Math.random() * 35) });
  await compose.dispose();
  await composeEl.dispose();
  await humanDelay();
  await page.keyboard.press('Enter');
  await delay(1500);
  return { ok: true };
}

module.exports = { navigateToDmThread, sendPlainTextInThread, delay, humanDelay };
