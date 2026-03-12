#!/usr/bin/env node
/**
 * Test first_name variable substitution without sending any DMs.
 * Run: node scripts/test-first-name-variables.js
 */
const { substituteVariables, normalizeName } = require('../utils/message-variables');

const template = 'Hey {{first_name}}! ☀️ Hast du Kapazitäten im Q1?';

const testLeads = [
  { username: 'vladamia.ugc', display_name: 'Content Creator • Vlada • UGC' },
  { username: 'nataly_jovner', display_name: 'Nataly | UGC & Brand Content 📍USA Texas' },
  { username: 'jennys.reallife', first_name: 'JENNIFER*!' },
  { username: 'littlemilaugc', display_name: 'Mila✨ UGC Creator | Kids & Family Brands' },
  { username: 'john_doe', first_name: null, last_name: null },
  { username: 'comment_scrape_user', first_name: null, last_name: null, display_name: null },
];

console.log('Template:', template);
console.log('---\n');

testLeads.forEach((lead, i) => {
  const result = substituteVariables(template, lead);
  console.log(`Lead ${i + 1}: @${lead.username}`);
  console.log(`  display_name: ${lead.display_name ?? '(none)'}`);
  console.log(`  first_name:   ${lead.first_name ?? '(none)'}`);
  console.log(`  Result:       ${result}`);
  console.log('');
});

console.log('---\nnormalizeName tests:');
['Mila✨', 'JENNIFER*!', '•!', 'Mary-Jane'].forEach((s) => {
  console.log(`  "${s}" → "${normalizeName(s)}"`);
});
