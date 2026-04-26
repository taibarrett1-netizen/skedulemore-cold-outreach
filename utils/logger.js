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
  const fileLine = `[${timestamp()}] [${level}] ${msg}\n`;
  const stdoutHasPm2Timestamp =
    process.env.pm_id != null &&
    process.env.LOG_STDOUT_TIMESTAMP !== '1' &&
    process.env.LOG_STDOUT_TIMESTAMP !== 'true';
  const stdoutLine = stdoutHasPm2Timestamp ? `[${level}] ${msg}\n` : fileLine;
  process.stdout.write(stdoutLine);
  logStream.write(fileLine);
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
