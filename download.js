const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const { pipeline } = require('stream/promises');
const { Readable } = require('stream');

const LIST_FILE = path.resolve(__dirname, 'list.txt');
const TEMP_DIR = path.resolve(__dirname, 'temp');
const CONCURRENCY = Number(process.env.CONCURRENCY) > 0 ? Number(process.env.CONCURRENCY) : 3;
const TIMEOUT_MS = Number(process.env.TIMEOUT) > 0 ? Number(process.env.TIMEOUT) : 60_000;
const RETRY = Number.isInteger(Number(process.env.RETRY)) ? Number(process.env.RETRY) : 2;

/** 解析清单文件 */
function parseList(text) {
  return text
    .split(/\r?\n/)
    .map((raw, i) => ({ lineNo: i + 1, raw }))
    .filter(({ raw }) => {
      const line = raw.trim();
      return line !== '' && !line.startsWith('#') && line.includes(',');
    })
    .map(({ lineNo, raw }) => {
      const line = raw.trim();
      const idx = line.indexOf(',');
      const relPath = line.slice(0, idx).trim();
      const url = line.slice(idx + 1).trim();
      if (!relPath || !url) {
        throw new Error(`list.txt 第 ${lineNo} 行格式错误，路径或 URL 为空：${line}`);
      }
      return { lineNo, relPath, url };
    });
}

/** 归一化为 temp/ 下的绝对路径，并阻止 ../ 越界 */
function safeTarget(relPath) {
  const target = path.resolve(TEMP_DIR, relPath);
  if (target !== TEMP_DIR && !target.startsWith(TEMP_DIR + path.sep)) {
    throw new Error(`非法路径（越出 temp 目录）：${relPath}`);
  }
  return target;
}

async function downloadOne(task) {
  const { relPath, url, lineNo } = task;
  const target = safeTarget(relPath);

  for (let attempt = 1; attempt <= RETRY + 1; attempt++) {
    let tmpFile;
    try {
      await fsp.mkdir(path.dirname(target), { recursive: true });
      tmpFile = `${target}.part-${process.pid}-${Date.now()}`;

      const res = await fetch(url, {
        signal: AbortSignal.timeout(TIMEOUT_MS),
        headers: { 'user-agent': 'node-downloader/1.0' },
      });
      if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
      if (!res.body) throw new Error('响应没有内容');

      await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(tmpFile));
      await fsp.rename(tmpFile, target);

      const { size } = await fsp.stat(target);
      console.log(`[OK]  ${relPath}  (${size} bytes)  <- ${url}`);
      return { ok: true, task };
    } catch (err) {
      if (tmpFile) await fsp.rm(tmpFile, { force: true });
      if (attempt <= RETRY) {
        console.warn(`[重试 ${attempt}/${RETRY}] 第 ${lineNo} 行 ${relPath}：${err.message}`);
        await new Promise((r) => setTimeout(r, 500 * attempt));
        continue;
      }
      console.error(`[失败] 第 ${lineNo} 行 ${relPath} <- ${url}\n       ${err.message}`);
      return { ok: false, task, error: err };
    }
  }
}

/** 固定并发的任务调度 */
async function runPool(tasks, limit) {
  const results = [];
  let cursor = 0;
  const workers = Array.from({ length: Math.min(limit, tasks.length) }, async () => {
    while (cursor < tasks.length) {
      const task = tasks[cursor++];
      results.push(await downloadOne(task));
    }
  });
  await Promise.all(workers);
  return results;
}

(async () => {
  let text;
  try {
    text = await fsp.readFile(LIST_FILE, 'utf8');
  } catch {
    console.error(`找不到清单文件：${LIST_FILE}`);
    process.exit(1);
  }

  let tasks;
  try {
    tasks = parseList(text);
  } catch (err) {
    console.error(err.message);
    process.exit(1);
  }

  if (tasks.length === 0) {
    console.log('清单为空，无需下载。');
    return;
  }

  await fsp.mkdir(TEMP_DIR, { recursive: true });
  console.log(`共 ${tasks.length} 个文件，并发 ${CONCURRENCY}，输出目录：${TEMP_DIR}\n`);

  const started = Date.now();
  const results = await runPool(tasks, CONCURRENCY);
  const failed = results.filter((r) => !r.ok);

  console.log(`\n完成：${results.length - failed.length}/${results.length}，耗时 ${((Date.now() - started) / 1000).toFixed(1)}s`);
  if (failed.length) {
    console.log('失败清单：');
    failed.forEach((f) => console.log(`  - ${f.task.relPath}  ${f.task.url}`));
    process.exit(1);
  }
})();