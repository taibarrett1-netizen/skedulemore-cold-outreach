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
 *   displayName?: string | null,
 * }>}
 */
async function clickInstagramDmSearchResult(page, username) {
  const u = String(username || '').trim().replace(/^@/, '');
  if (!u) {
    return { ok: false, reason: 'search_result_select_failed', detail: 'empty_username', logLine: 'empty username' };
  }
  return page.evaluate((needleRaw) => {
    const needle = needleRaw.toLowerCase();
    const needleEsc = needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    // Word-boundary token match — checks visible text only (NOT hrefs).
    const needleTokenRe = new RegExp(`(^|[^a-z0-9._])@?${needleEsc}([^a-z0-9._]|$)`, 'i');
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

    // Text-only combined match — deliberately excludes href to avoid false positives from
    // URL query params like utm_source=<username> appearing in thread items.
    function combinedMatchText(el) {
      const bits = [el.textContent || '', el.getAttribute('aria-label') || '', el.getAttribute('title') || ''];
      return bits.join(' ').toLowerCase();
    }

    function isReservedPathSegment(seg) {
      const bad = new Set(['direct', 'p', 'reel', 'reels', 'stories', 'explore', 'accounts', 'legal', 'api', 'tv']);
      return !seg || bad.has(seg.toLowerCase());
    }

    function hrefProfileUsername(href) {
      if (!href || typeof href !== 'string') return null;
      if (!href.toLowerCase().includes('instagram.com')) return null;
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
      return hrefProfileUsername(href) === needle;
    }

    function resolveInstagramHref(el) {
      if (!el) return '';
      if (el.href && String(el.href).includes('instagram.com')) return el.href;
      const inner = el.querySelector && el.querySelector('a[href*="instagram.com"]');
      if (inner && inner.href) return inner.href;
      const a = el.closest && el.closest('a[href*="instagram.com"]');
      return a && a.href ? a.href : '';
    }

    function isChromeOnlyRow(el) {
      const raw = (el.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase();
      const shortNav = /^(back|next|close|cancel|done|ok|chat|not now|compose|skip|new message|new|clear search)$/i;
      if (raw.length <= 2) return true;
      if (raw.length <= 48 && shortNav.test(raw)) return true;
      return false;
    }

    /**
     * Returns true if the element's visible text contains the target username as a word-boundary token.
     * Deliberately does NOT check the href — that was the root cause of false positives against
     * existing thread rows that had utm_source=<username> in their URL.
     */
    function rowLooksLikeSearchHit(el) {
      if (isChromeOnlyRow(el)) return false;
      const c = combinedMatchText(el);
      if (c.includes('more accounts')) return false;
      // Must contain the username as a standalone token in visible text.
      return needleTokenRe.test(c);
    }

    /**
     * Extract the display name from a search result row.
     * The sidebar row text structure is typically:
     *   Line 1: Display Name  (e.g. "Tai - SkeduleMore")
     *   Line 2: username      (e.g. "skedulemore")
     *   Line 3: bio/tagline   (e.g. "Scale Without Setters")
     *
     * We find the line that equals the username and take the line immediately above it.
     * Returns null if display name equals the username or cannot be determined.
     */
    function extractDisplayNameFromRow(el) {
      const rawText = (el.innerText || el.textContent || '').replace(/\r/g, '').trim();
      const lines = rawText
        .split(/\n+/)
        .map((s) => s.replace(/\s+/g, ' ').trim())
        .filter(Boolean);

      const userIdx = lines.findIndex((l) => l.toLowerCase().replace(/^@/, '') === needle);
      if (userIdx > 0) {
        const candidate = lines[userIdx - 1];
        // Sanity: not a chrome label, not too long
        if (
          candidate &&
          candidate.toLowerCase() !== 'more accounts' &&
          candidate.length >= 1 &&
          candidate.length <= 120
        ) {
          return candidate; // e.g. "Tai - SkeduleMore"
        }
      }
      // Username might be first line — display name may differ by capitalisation or be the same.
      // In this case return null; the caller can fall back to the username.
      return null;
    }

    /**
     * Broad candidate collection.
     * Includes bare div[role="button"] so that "More accounts" result rows (which Instagram
     * renders without a listbox wrapper on /direct/new) are also considered.
     */
    function collectCandidates() {
      const selectors = [
        '[role="listbox"] [role="option"]',
        '[role="listbox"] a[href*="instagram.com"]',
        '[role="listbox"] div[role="button"]',
        '[role="presentation"] [role="option"]',
        'div[role="dialog"] [role="option"]',
        'div[role="dialog"] a[href*="instagram.com/"]',
        'div[role="dialog"] div[role="button"]',
        // Broad fallback — catches IG layouts where search results sit outside listbox/dialog.
        'div[role="button"]',
        'button',
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

    function rowCenterY(el) {
      try {
        const r = el.getBoundingClientRect();
        return r.top + r.height / 2;
      } catch {
        return Number.POSITIVE_INFINITY;
      }
    }

    function nearestMoreAccountsHeadingY() {
      const headings = Array.from(document.querySelectorAll('*')).filter((el) => {
        if (!visible(el)) return false;
        const t = (el.textContent || '').replace(/\s+/g, ' ').trim().toLowerCase();
        return t === 'more accounts';
      });
      if (!headings.length) return null;
      let best = null;
      for (const h of headings) {
        const y = rowCenterY(h);
        if (!Number.isFinite(y)) continue;
        if (best == null || y < best) best = y;
      }
      return best;
    }

    const moreAccountsY = nearestMoreAccountsHeadingY();

    // Sort helper: prioritise rows visually below "More accounts" heading (those are the
    // search-result accounts, not existing thread rows), then by vertical position.
    function sortByMoreAccounts(arr) {
      return [...arr].sort((a, b) => {
        const ay = rowCenterY(a);
        const by = rowCenterY(b);
        if (moreAccountsY != null) {
          const aInMore = ay > moreAccountsY + 8 ? 1 : 0;
          const bInMore = by > moreAccountsY + 8 ? 1 : 0;
          if (aInMore !== bInMore) return bInMore - aInMore;
        }
        return ay - by;
      });
    }

    const candidates = collectCandidates();
    const sorted = sortByMoreAccounts(candidates);

    // ── Pass 1: exact href match (safest — no text ambiguity) ──
    const byHref = sorted.find((el) => {
      const h = resolveInstagramHref(el);
      return h && hrefMatches(h);
    });
    if (byHref) {
      const displayName = extractDisplayNameFromRow(byHref);
      let clickTarget = byHref;
      if (byHref.tagName !== 'A') {
        const inner = byHref.querySelector && byHref.querySelector('a[href*="instagram.com"]');
        const outer = byHref.closest && byHref.closest('a[href*="instagram.com"]');
        clickTarget = inner || outer || byHref;
      }
      clickTarget.click();
      return { ok: true, detail: 'href_match', displayName: displayName || null };
    }

    // ── Pass 2: username token in visible text (word-boundary, no href, "More accounts" first) ──
    const byText = sorted.find((el) => rowLooksLikeSearchHit(el));
    if (byText) {
      const displayName = extractDisplayNameFromRow(byText);
      byText.click();
      return { ok: true, detail: 'text_token_match', displayName: displayName || null };
    }

    // ── Diagnostics ──
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
      return { ok: false, reason: 'account_private', detail: 'page_text_during_search', logLine: `handle=${needleRaw}; private_hint_in_body=true` };
    }
    if (lowerBody.includes('try again later') || lowerBody.includes('too many')) {
      return { ok: false, reason: 'rate_limited', detail: 'page_text_during_search', logLine: `handle=${needleRaw}; rate_limit_hint_in_body=true` };
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
    return { ok: false, reason, detail, logLine: parts.join('; ') };
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
