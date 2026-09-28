#!/usr/bin/env node
// 文档站链接完整性校验——补 VitePress build 的两个盲区：
//   1) 跨页 #锚点：build 的死链检查只验页面存在，不验锚点；
//   2) 路径大小写：Pages 对大小写敏感，而 macOS 开发机的文件系统不敏感，
//      错大小写在本地 dev/preview 全绿、上线即 404（DESIGN/NOTES 踩过）。
// 锚点真值取自 dist 渲染产物里的 id= 属性（VitePress 自己生成的 slug，
// 免得在本脚本里复刻 slugify 规则）——所以先 `pnpm build` 再跑本检查，
// pages workflow 里的步骤顺序即此依赖。外部 http(s) 链接不在此校验
// （CI 内联网探测是抖动源），靠人工/发版前抽查。
// 用法：pnpm check:links（零依赖，Node 标准库）。
import { readdirSync, readFileSync, existsSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../.."); // 仓库根
const docsDir = join(root, "docs");
const distDir = join(docsDir, ".vitepress", "dist");

const problems = [];
let checked = 0;
let skippedExternal = 0;

if (!existsSync(distDir)) {
  console.error("dist 不存在：先 `pnpm build` 再跑链接校验（锚点真值在渲染产物里）。");
  process.exit(1);
}

/** 剥掉围栏代码块与行内代码，避免把示例文本当链接。 */
function stripCode(md) {
  return md
    .replace(/^(```|~~~)[^\n]*\n[\s\S]*?^\1[^\n]*$/gm, "")
    .replace(/`[^`\n]*`/g, "");
}

/** 逐级目录做大小写核对：任一环节仅大小写不同即报 case mismatch。 */
function caseExactPath(absPath) {
  let cur = absPath;
  const tail = [];
  while (cur !== root && cur !== "/") {
    const base = dirname(cur);
    const names = readdirSync(base);
    const hit = names.find((n) => n === cur.slice(base.length + 1));
    if (!hit) {
      const ci = names.find(
        (n) => n.toLowerCase() === cur.slice(base.length + 1).toLowerCase(),
      );
      if (ci) return { ok: false, caseIssue: true, actual: join(base, ci) };
      return { ok: false, caseIssue: false };
    }
    tail.unshift(hit);
    cur = base;
  }
  return { ok: true, path: join(cur, ...tail) };
}

/** 解析一个内部链接目标：返回 { file, anchor } 或 null（外部链接）。 */
function resolveTarget(fromFile, raw) {
  let target = raw.split("?")[0];
  if (/^(https?:|mailto:)/.test(target)) return null;
  let anchor = "";
  const hash = target.indexOf("#");
  if (hash >= 0) {
    anchor = target.slice(hash + 1);
    target = target.slice(0, hash);
  }
  const abs =
    target === "" && anchor
      ? fromFile
      : target.startsWith("/")
        ? join(docsDir, target)
        : join(dirname(fromFile), target);
  if (abs.endsWith("/") || abs === docsDir) return { file: join(docsDir, "index.md"), anchor };
  return { file: abs, anchor };
}

/** dist 里对应某个源 md 的 HTML 及其全部 id。 */
const idCache = new Map();
function distIds(srcFile) {
  if (idCache.has(srcFile)) return idCache.get(srcFile);
  const rel = relative(docsDir, srcFile).replace(/\.md$/, ".html");
  const htmlPath = join(distDir, rel);
  let ids = null;
  if (existsSync(htmlPath)) {
    ids = new Set([...readFileSync(htmlPath, "utf8").matchAll(/\sid="([^"]+)"/g)].map((m) => m[1]));
  }
  idCache.set(srcFile, ids);
  return ids;
}

function deadLinkMsg(label, raw, ce, file) {
  const why = ce.caseIssue ? `大小写不匹配，实际是 ${relative(root, ce.actual)}` : "文件不存在";
  return `${label}: 死链 \`${raw}\`（${why} → ${relative(root, file)}）`;
}

/** 站点根路径的静态资产落在 docs/public/（VitePress 约定），那里存在即放行。 */
function inPublic(file) {
  const rel = relative(docsDir, file);
  if (rel.startsWith("..")) return { ok: false, caseIssue: false };
  return caseExactPath(join(docsDir, "public", rel));
}

function checkLink(fromFile, raw, label) {
  const t = resolveTarget(fromFile, raw);
  if (t === null) {
    skippedExternal++;
    return;
  }
  checked++;
  let file = t.file;
  const ce = caseExactPath(file);
  if (!ce.ok) {
    if (/\.md$/.test(file)) {
      if (inPublic(file).ok) return;
      problems.push(deadLinkMsg(label, raw, ce, file));
      return;
    }
    // 无扩展名：优先按 cleanUrls 页面补 .md；仍不存在才报死链（或 public 资产放行）。
    const ceMd = caseExactPath(file + ".md");
    if (!ceMd.ok) {
      if (inPublic(file).ok) return;
      problems.push(deadLinkMsg(label, raw, ce.caseIssue ? ce : ceMd, file));
      return;
    }
    file += ".md";
  } else if (!/\.md$/.test(file)) {
    // 存在但无扩展名：cleanUrls 页面（补 .md 继续验锚点）或静态资产（到此为止）。
    const ceMd = caseExactPath(file + ".md");
    if (ceMd.ok) file += ".md";
    else return;
  }
  if (!t.anchor) return;
  const ids = distIds(file);
  if (ids === null) {
    problems.push(`${label}: \`${raw}\` 的目标页面没有构建产物（${relative(root, file)}）`);
  } else if (!ids.has(t.anchor)) {
    problems.push(`${label}: 锚点不存在 \`${raw}\`（页面里没有 id="${t.anchor}"）`);
  }
}

const mdLink = /\[[^\]]*\]\(([^)\s]+)(?:\s+"[^"]*")?\)/g;

for (const name of readdirSync(docsDir).filter((n) => n.endsWith(".md")).sort()) {
  const file = join(docsDir, name);
  const src = readFileSync(file, "utf8");
  const fm = src.match(/^---\n([\s\S]*?)\n---/);
  if (fm) for (const m of fm[1].matchAll(/^\s*link:\s*(\S+)\s*$/gm)) checkLink(file, m[1], `docs/${name} frontmatter`);
  for (const m of stripCode(src).matchAll(mdLink)) checkLink(file, m[1], `docs/${name}`);
}

// README：只校验仓库内相对路径的链接（在线列与徽章是外链，不进 CI 联网探测）。
{
  const file = join(root, "README.md");
  const src = readFileSync(file, "utf8");
  for (const m of stripCode(src).matchAll(mdLink)) {
    if (!/^(https?:|mailto:)/.test(m[1])) checkLink(file, m[1], "README.md");
  }
}

if (problems.length) {
  console.error(`链接校验失败，${problems.length} 处问题：`);
  for (const p of problems) console.error("  - " + p);
  process.exit(1);
}
console.log(`链接校验通过：${checked} 个内链（含 frontmatter link:），锚点对 dist 真值；外部链接 ${skippedExternal} 个跳过（不联网探测）。`);
