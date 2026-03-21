/**
 * Mobile UA and viewport for Instagram automation.
 * Mimics mobile devices to reduce desktop-bot fingerprinting.
 */
const MOBILE_UAS = [
  'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1',
  'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1',
  'Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Mobile Safari/537.36',
];

function getRandomMobileUA() {
  return MOBILE_UAS[Math.floor(Math.random() * MOBILE_UAS.length)];
}

const MOBILE_VIEWPORT = { width: 390, height: 844, isMobile: true, hasTouch: true, deviceScaleFactor: 2 };
const DESKTOP_VIEWPORT = { width: 1280, height: 800, isMobile: false, hasTouch: false, deviceScaleFactor: 1 };
const DESKTOP_UA =
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

async function applyMobileEmulation(page) {
  await page.setUserAgent(getRandomMobileUA());
  await page.setViewport(MOBILE_VIEWPORT);
}

async function applyDesktopEmulation(page) {
  await page.setUserAgent(DESKTOP_UA);
  await page.setViewport(DESKTOP_VIEWPORT);
}

module.exports = { getRandomMobileUA, MOBILE_VIEWPORT, DESKTOP_VIEWPORT, applyMobileEmulation, applyDesktopEmulation };
