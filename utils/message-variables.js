/**
 * Replaces {{variable}} placeholders in message text with lead data.
 * Supported: {{username}}, {{first_name}}, {{last_name}}, {{full_name}}. {{instagram_username}} = {{username}}.
 * If display_name exists: take first full word, then normalise. Else first_name, else derive from username.
 */
function normalizeName(str) {
  if (!str || typeof str !== 'string') return '';
  let s = str.trim();
  if (!s) return '';
  s = s.replace(/[\u{1F300}-\u{1F9FF}\u{2600}-\u{26FF}\u{2700}-\u{27BF}\u{FE00}-\u{FE0F}\u{1F000}-\u{1F02F}\u{1F0A0}-\u{1F0FF}\u{1F600}-\u{1F64F}\u{1F680}-\u{1F6FF}]/gu, '');
  s = s.replace(/[^\p{L}\p{N}\s-]/gu, '');
  s = s.trim();
  if (!s) return '';
  return s.charAt(0).toUpperCase() + s.slice(1).toLowerCase();
}

function substituteVariables(text, lead = {}) {
  if (!text || typeof text !== 'string') return text;
  const username = (lead.username || '').trim().replace(/^@/, '') || '';

  let first = '';
  let last = (lead.last_name || '').trim();
  if (lead.display_name && typeof lead.display_name === 'string') {
    const firstWord = lead.display_name.trim().split(/\s+/)[0] || '';
    first = normalizeName(firstWord);
  }
  if (!first && (lead.first_name || '').trim()) {
    first = normalizeName((lead.first_name || '').trim());
  }
  if (!first && !last && username) {
    const parts = username.split(/[_.\s-]+/).filter(Boolean);
    first = parts[0] ? normalizeName(parts[0]) : normalizeName(username);
    last = parts.length > 1 ? parts.slice(1).map((p) => normalizeName(p)).join(' ') : '';
  }
  if (last) last = normalizeName(last);

  const fullName = [first, last].filter(Boolean).join(' ') || username;

  const vars = {
    username,
    instagram_username: username,
    first_name: first,
    last_name: last,
    full_name: fullName,
  };

  return text.replace(/\{\{\s*(\w+)\s*\}\}/gi, (_, key) => vars[key.toLowerCase()] ?? `{{${key}}}`);
}

module.exports = { substituteVariables, normalizeName };
