const fs = require('fs');
const path = require('path');

const logsDir = path.join(__dirname, '..', 'logs');
if (!fs.existsSync(logsDir)) fs.mkdirSync(logsDir, { recursive: true });

const logPath = path.join(logsDir, 'bot.log');
const errorPath = path.join(logsDir, 'error.log');
const logStream = fs.createWriteStream(logPath, { flags: 'a' });
const errorStream = fs.createWriteStream(errorPath, { flags: 'a' });

function timestamp() {
  return new Date().toISOString();
}

function write(level, msg) {
  const line = `[${timestamp()}] [${level}] ${msg}\n`;
  process.stdout.write(line);
  logStream.write(line);
}

function log(msg) {
  write('INFO', msg);
}

function warn(msg) {
  write('WARN', msg);
}

function error(msg, err) {
  const errMsg = err ? (err.message || String(err)) : '';
  write('ERROR', errMsg ? `${msg}: ${errMsg}` : msg);
  const errLine = err && err.stack ? `${msg}\n${err.stack}\n` : `${msg}\n`;
  errorStream.write(`[${timestamp()}] ${errLine}`);
}

module.exports = { log, warn, error };
