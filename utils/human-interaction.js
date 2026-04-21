const mousePositions = new WeakMap();

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function randomInt(min, max) {
  const lo = Math.ceil(Math.min(min, max));
  const hi = Math.floor(Math.max(min, max));
  return Math.floor(Math.random() * (hi - lo + 1)) + lo;
}

function chance(probability) {
  return Math.random() < probability;
}

function randomBetween(min, max) {
  return min + Math.random() * (max - min);
}

function jitter(value, spread) {
  return value + randomBetween(-spread, spread);
}

function pauseRange(kind = 'default') {
  switch (kind) {
    case 'pre_click':
      return [120, 380];
    case 'post_click':
      return [180, 520];
    case 'between_actions':
      return [450, 1450];
    case 'open_dm':
      return [900, 2400];
    case 'compose':
      return [700, 1800];
    case 'post_send':
      return [1300, 3200];
    case 'scroll':
      return [400, 1200];
    case 'micro':
      return [50, 160];
    default:
      return [300, 900];
  }
}

async function organicPause(kind = 'default', multiplier = 1) {
  const [min, max] = pauseRange(kind);
  await delay(Math.round(randomInt(min, max) * Math.max(0.2, multiplier)));
}

function getViewportBounds(page) {
  const vp = typeof page.viewport === 'function' ? page.viewport() : null;
  return {
    width: Math.max(320, vp?.width || 1280),
    height: Math.max(300, vp?.height || 800),
  };
}

function clampPointToViewport(page, point) {
  const vp = getViewportBounds(page);
  return {
    x: Math.max(4, Math.min(vp.width - 4, point.x)),
    y: Math.max(4, Math.min(vp.height - 4, point.y)),
  };
}

function getLastMousePosition(page) {
  const cached = mousePositions.get(page);
  if (cached) return cached;
  const vp = getViewportBounds(page);
  const initial = {
    x: Math.round(vp.width * randomBetween(0.18, 0.42)),
    y: Math.round(vp.height * randomBetween(0.14, 0.38)),
  };
  mousePositions.set(page, initial);
  return initial;
}

function rememberMousePosition(page, point) {
  mousePositions.set(page, { x: point.x, y: point.y });
}

function cubicBezierPoint(p0, p1, p2, p3, t) {
  const mt = 1 - t;
  const mt2 = mt * mt;
  const t2 = t * t;
  return {
    x: mt2 * mt * p0.x + 3 * mt2 * t * p1.x + 3 * mt * t2 * p2.x + t2 * t * p3.x,
    y: mt2 * mt * p0.y + 3 * mt2 * t * p1.y + 3 * mt * t2 * p2.y + t2 * t * p3.y,
  };
}

async function moveMouseNaturally(page, destination, opts = {}) {
  if (!page?.mouse || !destination) return;
  const start = getLastMousePosition(page);
  const end = clampPointToViewport(page, destination);
  const dx = end.x - start.x;
  const dy = end.y - start.y;
  const distance = Math.hypot(dx, dy);
  const steps = Math.max(12, Math.min(48, Math.round(distance / randomBetween(10, 22))));
  const bend = Math.max(18, Math.min(120, distance * randomBetween(0.14, 0.32)));
  const midX = start.x + dx * 0.5;
  const midY = start.y + dy * 0.5;
  const perpX = distance > 0 ? -dy / distance : 0;
  const perpY = distance > 0 ? dx / distance : 0;
  const curveDir = chance(0.5) ? 1 : -1;
  const c1 = clampPointToViewport(page, {
    x: jitter(start.x + dx * randomBetween(0.22, 0.34) + perpX * bend * curveDir, 8),
    y: jitter(start.y + dy * randomBetween(0.18, 0.3) + perpY * bend * curveDir, 8),
  });
  const c2 = clampPointToViewport(page, {
    x: jitter(midX + dx * randomBetween(0.12, 0.24) - perpX * bend * curveDir * randomBetween(0.55, 0.9), 10),
    y: jitter(midY + dy * randomBetween(0.12, 0.24) - perpY * bend * curveDir * randomBetween(0.55, 0.9), 10),
  });

  for (let i = 1; i <= steps; i++) {
    const rawT = i / steps;
    const easedT = rawT < 0.5 ? 2 * rawT * rawT : 1 - Math.pow(-2 * rawT + 2, 2) / 2;
    const point = cubicBezierPoint(start, c1, c2, end, easedT);
    await page.mouse.move(point.x, point.y);
    rememberMousePosition(page, point);
    const segmentDelay = Math.max(
      6,
      Math.round((opts.totalDurationMs || randomInt(180, 620)) / steps + randomBetween(-4, 10))
    );
    await delay(segmentDelay);
    if (i < steps - 1 && chance(0.08)) {
      await delay(randomInt(18, 85));
    }
  }

  rememberMousePosition(page, end);
}

async function moveMouseToElement(page, elementHandle, opts = {}) {
  if (!elementHandle) return false;
  const box = await elementHandle.boundingBox().catch(() => null);
  if (!box) return false;
  const padX = Math.max(3, Math.min(16, box.width * 0.18));
  const padY = Math.max(3, Math.min(12, box.height * 0.18));
  const point = {
    x: randomBetween(box.x + padX, box.x + Math.max(padX + 1, box.width - padX)),
    y: randomBetween(box.y + padY, box.y + Math.max(padY + 1, box.height - padY)),
  };
  await moveMouseNaturally(page, point, opts);
  return true;
}

async function clickElementNaturally(page, elementHandle, opts = {}) {
  if (!page || !elementHandle) return false;
  await elementHandle.evaluate((el) => {
    try {
      el.scrollIntoView({ block: 'center', inline: 'center', behavior: 'instant' });
    } catch {}
  }).catch(() => {});
  await organicPause('micro');
  const moved = await moveMouseToElement(page, elementHandle, opts).catch(() => false);
  if (!moved) {
    await elementHandle.click({ delay: randomInt(60, 140) }).catch(() => {});
    await organicPause('post_click');
    return true;
  }
  if (chance(0.3)) {
    await organicPause('pre_click', 0.5);
  } else {
    await organicPause('pre_click');
  }
  await page.mouse.down().catch(() => {});
  await delay(randomInt(35, 110));
  await page.mouse.up().catch(() => {});
  await organicPause('post_click');
  return true;
}

async function clickSelectorNaturally(page, selector, opts = {}) {
  const handle = await page.$(selector);
  if (!handle) return false;
  try {
    return await clickElementNaturally(page, handle, opts);
  } finally {
    await handle.dispose().catch(() => {});
  }
}

function typoForCharacter(ch) {
  const lower = ch.toLowerCase();
  const nearby = {
    a: ['s', 'q', 'z'],
    b: ['v', 'n', 'h'],
    c: ['x', 'v', 'd'],
    d: ['s', 'f', 'e'],
    e: ['w', 'r', 'd'],
    f: ['d', 'g', 'r'],
    g: ['f', 'h', 't'],
    h: ['g', 'j', 'y'],
    i: ['u', 'o', 'k'],
    j: ['h', 'k', 'u'],
    k: ['j', 'l', 'i'],
    l: ['k', 'o', 'p'],
    m: ['n', 'j', 'k'],
    n: ['b', 'm', 'h'],
    o: ['i', 'p', 'l'],
    p: ['o', 'l'],
    q: ['w', 'a'],
    r: ['e', 't', 'f'],
    s: ['a', 'd', 'w'],
    t: ['r', 'y', 'g'],
    u: ['y', 'i', 'j'],
    v: ['c', 'b', 'f'],
    w: ['q', 'e', 's'],
    x: ['z', 'c', 's'],
    y: ['t', 'u', 'h'],
    z: ['x', 'a'],
  };
  const options = nearby[lower];
  if (!options?.length) return null;
  const pick = options[randomInt(0, options.length - 1)];
  return ch === lower ? pick : pick.toUpperCase();
}

async function pressModifiedEnter(page, useShift = false) {
  if (useShift) await page.keyboard.down('Shift');
  await page.keyboard.press('Enter');
  if (useShift) await page.keyboard.up('Shift');
}

async function typeTextNaturally(page, text, opts = {}) {
  const value = String(text ?? '');
  const correctionChance = opts.correctionChance ?? 0.035;
  const pauseChance = opts.pauseChance ?? 0.11;
  const minKeyDelay = opts.minKeyDelay ?? 35;
  const maxKeyDelay = opts.maxKeyDelay ?? 135;
  const shiftEnterNewlines = opts.shiftEnterNewlines !== false;

  for (let i = 0; i < value.length; i++) {
    const ch = value[i];
    if (ch === '\n') {
      await pressModifiedEnter(page, shiftEnterNewlines);
      await delay(randomInt(60, 180));
      continue;
    }

    const shouldCorrect =
      /[a-z]/i.test(ch) &&
      i > 1 &&
      i < value.length - 1 &&
      !/\s/.test(value[i - 1] || '') &&
      chance(correctionChance);
    if (shouldCorrect) {
      const typo = typoForCharacter(ch);
      if (typo) {
        await page.keyboard.type(typo);
        await delay(randomInt(40, 120));
        await page.keyboard.press('Backspace');
        await delay(randomInt(50, 160));
      }
    }

    await page.keyboard.type(ch);
    await delay(randomInt(minKeyDelay, maxKeyDelay));

    if (pauseChance > 0 && chance(pauseChance)) {
      await delay(randomInt(120, 420));
    }
  }
}

async function focusAndTypeNaturally(page, elementHandle, text, opts = {}) {
  if (!page || !elementHandle) return false;
  await clickElementNaturally(page, elementHandle, { totalDurationMs: randomInt(220, 520) }).catch(() => {});
  if (opts.clearFirst) {
    await page.keyboard.down('Meta').catch(async () => {
      await page.keyboard.down('Control').catch(() => {});
    });
    await page.keyboard.press('KeyA').catch(() => {});
    await page.keyboard.up('Meta').catch(async () => {
      await page.keyboard.up('Control').catch(() => {});
    });
    await delay(randomInt(40, 120));
    await page.keyboard.press('Backspace').catch(() => {});
    await delay(randomInt(60, 140));
  }
  await organicPause(opts.beforeTypePause || 'micro');
  await typeTextNaturally(page, text, opts);
  return true;
}

async function randomMouseDrift(page, opts = {}) {
  if (!page?.mouse) return;
  const vp = getViewportBounds(page);
  const point = {
    x: randomBetween(vp.width * 0.12, vp.width * 0.88),
    y: randomBetween(vp.height * 0.12, vp.height * 0.72),
  };
  await moveMouseNaturally(page, point, { totalDurationMs: opts.totalDurationMs || randomInt(160, 420) }).catch(() => {});
}

async function naturalScrollPage(page, opts = {}) {
  if (!page) return;
  const rounds = Math.max(1, opts.rounds || randomInt(1, 3));
  const direction = opts.direction === 'up' ? -1 : 1;
  for (let i = 0; i < rounds; i++) {
    const delta = Math.round(randomBetween(180, 620)) * direction;
    if (page.mouse?.wheel) {
      await page.mouse.wheel({ deltaY: delta }).catch(() => {});
    } else {
      await page.evaluate((dy) => window.scrollBy(0, dy), delta).catch(() => {});
    }
    await organicPause('scroll', opts.pauseMultiplier || 1);
    if (chance(0.22)) {
      const settleDelta = Math.round(delta * -randomBetween(0.12, 0.3));
      if (page.mouse?.wheel) await page.mouse.wheel({ deltaY: settleDelta }).catch(() => {});
      else await page.evaluate((dy) => window.scrollBy(0, dy), settleDelta).catch(() => {});
      await delay(randomInt(140, 340));
    }
  }
}

async function idleMouseDrift(page, opts = {}) {
  if (!page?.mouse) return;
  const durationMs = Math.max(1500, opts.durationMs || randomInt(6000, 18000));
  const startedAt = Date.now();
  while (Date.now() - startedAt < durationMs) {
    await randomMouseDrift(page, {
      totalDurationMs: opts.segmentDurationMs || randomInt(500, 1800),
    }).catch(() => {});
    if (opts.allowPageScroll && chance(0.18)) {
      await naturalScrollPage(page, {
        rounds: 1,
        direction: chance(0.16) ? 'up' : 'down',
        pauseMultiplier: 0.5,
      }).catch(() => {});
    }
    const remaining = durationMs - (Date.now() - startedAt);
    if (remaining <= 0) break;
    await delay(Math.min(remaining, randomInt(900, 3200)));
  }
}

async function maybeLightStoryInteraction(page, opts = {}) {
  if (!page) return false;
  const openChance = opts.openChance ?? 0.18;
  const storyHandle = await page
    .$([
      'a[href*="/stories/"]',
      'button[aria-label*="story" i]',
      'canvas[aria-label*="story" i]',
      'img[alt*="story" i]',
    ].join(', '))
    .catch(() => null);
  if (!storyHandle) return false;
  try {
    await moveMouseToElement(page, storyHandle, { totalDurationMs: randomInt(180, 420) }).catch(() => {});
    await delay(randomInt(140, 320));
    if (!chance(openChance)) return true;
    await clickElementNaturally(page, storyHandle, { totalDurationMs: randomInt(200, 460) }).catch(() => {});
    await delay(randomInt(900, 2200));
    await page.keyboard.press('Escape').catch(() => {});
    await delay(randomInt(200, 600));
    return true;
  } finally {
    await storyHandle.dispose().catch(() => {});
  }
}

async function closeStoryViewer(page) {
  const closeHandle = await page
    .$([
      'button[aria-label="Close"]',
      'svg[aria-label="Close"]',
      '[role="button"][aria-label*="close" i]',
    ].join(', '))
    .catch(() => null);
  if (closeHandle) {
    try {
      await clickElementNaturally(page, closeHandle, { totalDurationMs: randomInt(180, 420) }).catch(() => {});
      await delay(randomInt(240, 700));
      return true;
    } finally {
      await closeHandle.dispose().catch(() => {});
    }
  }
  await page.keyboard.press('Escape').catch(() => {});
  await delay(randomInt(220, 600));
  return false;
}

async function viewStoriesNaturally(page, opts = {}) {
  if (!page) return false;
  const triggerHandle = await page
    .$([
      'a[href*="/stories/"]',
      'button[aria-label*="story" i]',
      'canvas[aria-label*="story" i]',
      'img[alt*="story" i]',
    ].join(', '))
    .catch(() => null);
  if (!triggerHandle) return false;

  const minStories = Math.max(1, opts.minStories || 2);
  const maxStories = Math.max(minStories, opts.maxStories || 4);
  const storiesToView = randomInt(minStories, maxStories);

  try {
    await moveMouseToElement(page, triggerHandle, { totalDurationMs: randomInt(220, 520) }).catch(() => {});
    await delay(randomInt(240, 700));
    await clickElementNaturally(page, triggerHandle, { totalDurationMs: randomInt(220, 520) }).catch(() => {});
    await delay(randomInt(1600, 3200));

    for (let index = 0; index < storiesToView; index++) {
      const dwellMs = randomInt(opts.minViewMs || 8000, opts.maxViewMs || 20000);
      const startedAt = Date.now();
      while (Date.now() - startedAt < dwellMs) {
        if (chance(0.4)) {
          await randomMouseDrift(page, { totalDurationMs: randomInt(300, 900) }).catch(() => {});
        }
        const remaining = dwellMs - (Date.now() - startedAt);
        if (remaining <= 0) break;
        await delay(Math.min(remaining, randomInt(1400, 4200)));
      }

      if (index >= storiesToView - 1) break;

      const nextHandle = await page
        .$([
          'button[aria-label*="Next" i]',
          '[role="button"][aria-label*="Next" i]',
          'svg[aria-label*="Next" i]',
        ].join(', '))
        .catch(() => null);

      if (nextHandle) {
        try {
          await clickElementNaturally(page, nextHandle, { totalDurationMs: randomInt(180, 420) }).catch(() => {});
        } finally {
          await nextHandle.dispose().catch(() => {});
        }
      } else {
        const vp = getViewportBounds(page);
        await moveMouseNaturally(
          page,
          {
            x: randomBetween(vp.width * 0.74, vp.width * 0.92),
            y: randomBetween(vp.height * 0.36, vp.height * 0.64),
          },
          { totalDurationMs: randomInt(280, 720) }
        ).catch(() => {});
        await page.mouse.down().catch(() => {});
        await delay(randomInt(30, 90));
        await page.mouse.up().catch(() => {});
      }

      await delay(randomInt(800, 1800));
    }

    await closeStoryViewer(page).catch(() => {});
    return true;
  } finally {
    await triggerHandle.dispose().catch(() => {});
  }
}

module.exports = {
  chance,
  clickElementNaturally,
  clickSelectorNaturally,
  delay,
  focusAndTypeNaturally,
  idleMouseDrift,
  moveMouseNaturally,
  moveMouseToElement,
  naturalScrollPage,
  organicPause,
  randomDelay: randomInt,
  randomMouseDrift,
  typeTextNaturally,
  maybeLightStoryInteraction,
  viewStoriesNaturally,
};
