async function sendVoiceNoteInThread(page, opts = {}) {
  const {
    holdMs = 7000,
    logger = null,
  } = opts;

  const micHandle = await page.evaluateHandle(() => {
    const lower = (s) => (s || '').toLowerCase();
    const visible = (el) => el && el.offsetParent !== null && el.offsetWidth > 0 && el.offsetHeight > 0;
    const candidates = Array.from(document.querySelectorAll('button, div[role="button"], span[role="button"]'));
    return (
      candidates.find((el) => {
        if (!visible(el)) return false;
        const label = lower(el.getAttribute('aria-label'));
        const title = lower(el.getAttribute('title'));
        const txt = lower(el.textContent);
        return (
          label.includes('microphone') ||
          label.includes('voice') ||
          title.includes('microphone') ||
          title.includes('voice') ||
          txt === 'voice' ||
          txt.includes('hold to record')
        );
      }) || null
    );
  });
  const micEl = micHandle.asElement();
  if (!micEl) {
    const denied = await page.evaluate(() => {
      const txt = (document.body && document.body.innerText ? document.body.innerText : '').toLowerCase();
      return txt.includes('allow microphone') || txt.includes('microphone access') || txt.includes('permission');
    }).catch(() => false);
    await micHandle.dispose().catch(() => {});
    return { ok: false, reason: denied ? 'voice_permission_denied' : 'voice_mic_not_found' };
  }

  await micEl.scrollIntoViewIfNeeded().catch(() => {});
  const box = await micEl.boundingBox();
  if (!box) {
    await micEl.dispose().catch(() => {});
    await micHandle.dispose().catch(() => {});
    return { ok: false, reason: 'voice_mic_not_found' };
  }

  if (logger) logger.log(`Voice mic found. Recording for ${Math.round(holdMs)} ms`);
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await page.mouse.down();
  await page.waitForTimeout(holdMs);
  await page.mouse.up();
  await micEl.dispose().catch(() => {});
  await micHandle.dispose().catch(() => {});

  await page.waitForTimeout(1000);
  const sent = await page.evaluate(() => {
    const lower = (s) => (s || '').toLowerCase();
    const visible = (el) => el && el.offsetParent !== null && el.offsetWidth > 0 && el.offsetHeight > 0;
    const clickables = Array.from(document.querySelectorAll('button, div[role="button"], span[role="button"], a'));
    const send = clickables.find((el) => {
      if (!visible(el)) return false;
      const label = lower(el.getAttribute('aria-label'));
      const title = lower(el.getAttribute('title'));
      const txt = lower(el.textContent);
      return label.includes('send') || title.includes('send') || txt === 'send';
    });
    if (send) {
      send.click();
      return true;
    }
    return false;
  });
  if (!sent) return { ok: false, reason: 'voice_send_button_not_found' };

  await page.waitForTimeout(1200);
  return { ok: true };
}

module.exports = { sendVoiceNoteInThread };
