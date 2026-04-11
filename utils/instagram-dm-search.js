/**
 * Instagram Web /direct/new — pick the search result row for a handle.
 * Injected into the page via page.evaluate (must stay self-contained).
 */

/**
 * @returns {Promise<{
 *   ok: boolean,
 *   reason?: string,
 *   detail?: string,
 *   logLine?: string,
 * }>}
 */
async function clickInstagramDmSearchResult(page, username) {
  const u = String(username || '').trim().replace(/^@/, '');
  if (!u) {
    return { ok: false, reason: 'search_result_select_failed', detail: 'empty_username', logLine: 'empty username' };
  }
  return page.evaluate((needleRaw) => {
    const needle = needleRaw.toLowerCase();
    const body = document.body && document.body.innerText ? document.body.innerText : '';
    const lowerBody = body.toLowerCase();

    const igExplicitEmpty =
      lowerBody.includes("couldn't find") ||
      lowerBody.includes('could not find') ||
      lowerBody.includes('no results') ||
      lowerBody.includes('no users found') ||
      lowerBody.includes('no user found');

    function visible(el) {
      try {
        if (!el || el.disabled) return false;
        const r = el.getClientRects();
        if (!r || !r.length) return false;
        return r[0].width > 0 && r[0].height > 0;
      } catch {
        return false;
      }
    }

    function combinedMatchText(el) {
      const bits = [el.textContent || '', el.getAttribute('aria-label') || '', el.getAttribute('title') || ''];
      let a = el.closest && el.closest('a[href*="instagram.com"]');
      if (!a && el.tagName === 'A') a = el;
      if (a && a.href) bits.push(a.href);
      return bits.join(' ').toLowerCase();
    }

    function isReservedPathSegment(seg) {
      const bad = new Set([
        'direct',
        'p',
        'reel',
        'reels',
        'stories',
        'explore',
        'accounts',
        'legal',
        'api',
        'tv',
      ]);
      return !seg || bad.has(seg.toLowerCase());
    }

    function hrefProfileUsername(href) {
      if (!href || typeof href !== 'string') return null;
      const h = href.toLowerCase();
      if (!h.includes('instagram.com')) return null;
      try {
        const path = new URL(href, 'https://www.instagram.com').pathname.replace(/^\/+|\/+$/g, '');
        const first = path.split('/')[0] || '';
        if (!first || isReservedPathSegment(first)) return null;
        return decodeURIComponent(first).replace(/^@/, '').toLowerCase();
      } catch {
        return null;
      }
    }

    function hrefMatches(href) {
      const seg = hrefProfileUsername(href);
      return seg === needle;
    }

    /** Short nav / header controls — not search result rows */
    function resolveInstagramHref(el) {
      if (!el) return '';
      if (el.href && String(el.href).includes('instagram.com')) return el.href;
      const inner = el.querySelector && el.querySelector('a[href*="instagram.com"]');
      if (inner && inner.href) return inner.href;
      const a = el.closest && el.closest('a[href*="instagram.com"]');
      return a && a.href ? a.href : '';
    }

    function isChromeOnlyRow(el, combined) {
      const raw = (el.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase();
      const shortNav = /^(back|next|close|cancel|done|ok|chat|not now|compose|skip|new message|new)$/i;
      if (raw.length <= 2) return true;
      if (raw.length <= 48 && shortNav.test(raw)) return true;
      if (combined.length < 80 && shortNav.test(combined.replace(/\s+/g, ' ').trim())) return true;
      return false;
    }

    function rowLooksLikeSearchHit(el) {
      const c = combinedMatchText(el);
      if (c.includes('more accounts')) return false;
      // Require a bounded username token; do not allow loose substring matches
      // from message previews/URLs (e.g. utm_source containing the needle).
      const tokenRe = new RegExp(`(^|[^a-z0-9._])@?${needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}([^a-z0-9._]|$)`, 'i');
      if (!tokenRe.test(c)) return false;
      if (isChromeOnlyRow(el, c)) return false;
      return true;
    }

    function collectCandidates() {
      const selectors = [
        '[role="listbox"] [role="option"]',
        '[role="listbox"] a[href*="instagram.com"]',
        '[role="listbox"] div[role="button"]',
        '[role="presentation"] [role="option"]',
        'div[role="dialog"] [role="option"]',
        'div[role="dialog"] a[href*="instagram.com/"]',
        'div[role="dialog"] div[role="button"]',
      ];
      const seen = new Set();
      const out = [];
      for (const sel of selectors) {
        let nodes;
        try {
          nodes = document.querySelectorAll(sel);
        } catch {
          continue;
        }
        for (let i = 0; i < nodes.length; i++) {
          const el = nodes[i];
          if (!el || seen.has(el)) continue;
          if (!visible(el)) continue;
          seen.add(el);
          out.push(el);
        }
      }
      return out;
    }

    /** Prefer profile links, then listbox options, then buttons with handle in text */
    const candidates = collectCandidates();

    const byHref = candidates.find((el) => {
      const h = resolveInstagramHref(el);
      return h && hrefMatches(h);
    });
    if (byHref) {
      let clickTarget = byHref;
      if (byHref.tagName !== 'A') {
        const inner = byHref.querySelector && byHref.querySelector('a[href*="instagram.com"]');
        const outer = byHref.closest && byHref.closest('a[href*="instagram.com"]');
        clickTarget = inner || outer || byHref;
      }
      clickTarget.click();
      return { ok: true, detail: 'href_profile_match' };
    }

    const byText = candidates.find((el) => rowLooksLikeSearchHit(el));
    if (byText) {
      byText.click();
      return { ok: true, detail: 'text_or_aria_match' };
    }

    /** Secondary: profile links under dialog/listbox only (avoid sidebar / global nav) */
    const linkRoots = Array.from(
      document.querySelectorAll('[role="listbox"], div[role="dialog"], [role="presentation"]')
    );
    const linkSeen = new Set();
    const links = [];
    for (let r = 0; r < linkRoots.length; r++) {
      linkRoots[r].querySelectorAll('a[href*="instagram.com/"]').forEach((a) => {
        if (!visible(a) || linkSeen.has(a)) return;
        linkSeen.add(a);
        links.push(a);
      });
    }
    const linkHit = links.find((a) => hrefMatches(a.href));
    if (linkHit) {
      linkHit.click();
      return { ok: true, detail: 'scan_links' };
    }

    const listbox = document.querySelector('[role="listbox"]');
    const optionSample = listbox
      ? Array.from(listbox.querySelectorAll('[role="option"], a, div[role="button"]'))
          .filter(visible)
          .slice(0, 5)
          .map((el) => (el.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 56))
          .filter(Boolean)
      : [];

    const btnSample = Array.from(document.querySelectorAll('div[role="button"]'))
      .filter(visible)
      .slice(0, 8)
      .map((el) => (el.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 48))
      .filter(Boolean);

    if (lowerBody.includes('this account is private') || lowerBody.includes('account is private') || lowerBody.includes('private account')) {
      return {
        ok: false,
        reason: 'account_private',
        detail: 'page_text_during_search',
        logLine: `handle=${needleRaw}; private_hint_in_body=true`,
      };
    }
    if (lowerBody.includes('try again later') || lowerBody.includes('too many')) {
      return {
        ok: false,
        reason: 'rate_limited',
        detail: 'page_text_during_search',
        logLine: `handle=${needleRaw}; rate_limit_hint_in_body=true`,
      };
    }

    const needleInBody = lowerBody.includes(needle);
    let reason = 'search_result_select_failed';
    let detail = 'no_clickable_match';

    if (igExplicitEmpty && !needleInBody) {
      reason = 'user_not_found';
      detail = 'instagram_empty_state';
    } else if (igExplicitEmpty && needleInBody) {
      reason = 'user_not_found';
      detail = 'instagram_says_empty_but_handle_appears_in_page_text';
    }

    const parts = [
      `handle=${needleRaw}`,
      `reason=${reason}`,
      `detail=${detail}`,
      `igSaysNoResults=${igExplicitEmpty}`,
      `handleInBody=${needleInBody}`,
      listbox ? `listboxSample=${optionSample.join(' | ') || '(none)'}` : 'listbox=absent',
      `divRoleButtonSample=${btnSample.join(' | ') || '(none)'}`,
    ];
    return {
      ok: false,
      reason,
      detail,
      logLine: parts.join('; '),
    };
  }, u);
}

/**
 * Human-readable line for logs / pageSnippet when search selection fails.
 * @param {string} username
 * @param {{ reason?: string, logLine?: string, detail?: string }} pick
 */
function formatSearchFailurePageSnippet(username, pick) {
  const u = String(username || '').trim().replace(/^@/, '');
  const diag = pick.logLine || pick.detail || '';
  if (pick.reason === 'user_not_found') {
    return `Instagram search reported no usable match for @${u}. ${diag}`;
  }
  if (pick.reason === 'account_private') {
    return `Page suggests private/restricted while on search. ${diag}`;
  }
  if (pick.reason === 'rate_limited') {
    return `Page suggests rate limit while on search. ${diag}`;
  }
  return `Could not click a search result row for @${u} (automation). ${diag}`;
}

module.exports = { clickInstagramDmSearchResult, formatSearchFailurePageSnippet };
