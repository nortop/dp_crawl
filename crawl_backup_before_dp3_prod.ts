import fs from "node:fs";
import path from "node:path";
import { parse as parseCsv } from "csv-parse/sync";
import pLimit from "p-limit";
import { chromium, Browser, BrowserContext, Locator, Page, Request } from "playwright";
import { getDomain } from "tldts";


// npm run crawl -- --input candidate.csv --out observations_raw.csv --evidence evidence --concurrency 3 --runs 2 --resume true


// ---------- CLI args ----------
type Args = {
  input: string;
  out: string;
  evidence: string;
  concurrency: number;
  runs: number;
  headful: boolean;
  timeoutMs: number;
  settleMs: number;
  locale: string;
  resume: boolean;
};

function parseArgs(argv: string[]): Args {
  const a: any = {};
  for (let i = 0; i < argv.length; i++) {
    const k = argv[i];
    if (!k.startsWith("--")) continue;
    const key = k.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      a[key] = true;
    } else {
      a[key] = next;
      i++;
    }
  }
  return {
    input: String(a.input ?? "candidate.csv"),
    out: String(a.out ?? "observations_raw.csv"),
    evidence: String(a.evidence ?? "evidence"),
    concurrency: Number(a.concurrency ?? 3),
    runs: Number(a.runs ?? 2),
    headful: Boolean(a.headful ?? false),
    timeoutMs: Number(a.timeoutMs ?? 30000),
    settleMs: Number(a.settleMs ?? 3500),
    locale: String(a.locale ?? "th-TH"),
    resume: Boolean(a.resume ?? true),
  };
}

const ARGS = parseArgs(process.argv.slice(2));

// ---------- Patterns ----------
const ACCEPT_RE = /(accept all|allow all|agree|i accept|ok|yes|ยอมรับทั้งหมด|ยอมรับ|ตกลง|อนุญาตทั้งหมด|ยินยอม)/i;
const REJECT_RE = /(reject all|decline all|deny all|do not accept|no thanks|ปฏิเสธทั้งหมด|ปฏิเสธ|ไม่ยอมรับ|ไม่อนุญาต|ไม่ยินยอม)/i;
const MANAGE_RE = /(manage|settings|preferences|options|customize|more info|ตั้งค่า|จัดการ|ตัวเลือก|การตั้งค่า|ปรับแต่ง|รายละเอียดเพิ่มเติม)/i;
const CLOSE_RE = /(\bclose\b|dismiss|x|ปิด|ไม่ตอนนี้|ภายหลัง)/i;

const COOKIE_KEYWORDS_RE = /(cookie|cookies|consent|privacy|tracking|คุกกี้|ความเป็นส่วนตัว|ความยินยอม|ติดตาม|โฆษณา)/i;

const TOGGLE_ANALYTICS_RE = /(analytics|statistics|measurement|performance|วิเคราะห์|สถิติ|ประสิทธิภาพ)/i;
const TOGGLE_ADS_RE = /(ads|advertis|marketing|target|personaliz|remarket|การตลาด|โฆษณา|กำหนดเป้าหมาย|ปรับแต่ง)/i;

const ENTER_SITE_RE =
  /(เข้าเว็บไซต์|เข้าสู่เว็บไซต์|ไปยังเว็บไซต์|เข้าหน้าเว็บไซต์|ไปยังหน้าเว็บไซต์|เริ่มใช้งาน|ดำเนินการต่อ|ต่อไป|ยืนยันและไปต่อ|Enter (the )?site|Continue( to)?( site)?|Proceed|Go to site|I understand|Agree and continue)/i;

  const CONFIRM_PREFS_RE =
  /(ยืนยันตัวเลือกของฉัน|ยืนยันตัวเลือก|บันทึก(การตั้งค่า)?|บันทึกและปิด|ตกลง|ยืนยัน|Save( settings)?|Confirm( choices)?|Accept selected|Allow selected)/i;

// กันไม่ให้ไปกด "ยอมรับทั้งหมด" โดยเผลอ
const ACCEPT_ALL_RE =
  /(ยอมรับทั้งหมด|accept all|allow all|agree|ตกลง|ok)/i;


// กันไม่ให้ไปกด "ยอมรับคุกกี้" แทน
const COOKIE_WORD_RE = /(cookie|cookies|คุกกี้)/i;


// ---------- Types ----------
type CandidateRow = {
  domain: string;
  stratum: string;
  source?: string;
  source_rank?: string;
};

type DeviceKind = "desktop" | "mobile";

type Obs = Record<string, any>;

// ---------- Helpers ----------
function ensureDir(p: string) {
  fs.mkdirSync(p, { recursive: true });
}

function nowIsoUtc() {
  return new Date().toISOString();
}

function safeSlug(s: string) {
  return s.replace(/[^a-zA-Z0-9._-]+/g, "_");
}

function normalizeDomain(raw: unknown): string {
  if (typeof raw !== "string") return "";
  let x = raw.trim();
  x = x.replace(/^https?:\/\//, "");
  x = x.split("/")[0];
  if (x.startsWith("www.")) x = x.slice(4);
  return x.toLowerCase();
}


function domainRoot(host: string): string {
  const d = getDomain(host);
  return d || host;
}

function hostOf(url: string): string {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return "";
  }
}

function isThirdParty(reqHost: string, siteHost: string): boolean {
  if (!reqHost || !siteHost) return false;
  const r1 = domainRoot(reqHost);
  const r2 = domainRoot(siteHost);
  return r1 !== r2;
}

function writeCsvRowAppend(outPath: string, header: string[], row: Record<string, any>) {
  const exists = fs.existsSync(outPath);
  const line = header
    .map((h) => {
      const v = row[h];
      const s = v === undefined || v === null ? "" : String(v);
      // CSV escape
      const esc = s.includes(",") || s.includes('"') || s.includes("\n");
      return esc ? `"${s.replace(/"/g, '""')}"` : s;
    })
    .join(",") + "\n";

  if (!exists) {
    fs.writeFileSync(outPath, header.join(",") + "\n", "utf-8");
  }
  fs.appendFileSync(outPath, line, "utf-8");
}

function loadExistingKeys(outPath: string): Set<string> {
  const keys = new Set<string>();
  if (!fs.existsSync(outPath)) return keys;
  const txt = fs.readFileSync(outPath, "utf-8");
  const lines = txt.split("\n").filter(Boolean);
  if (lines.length < 2) return keys;
  const header = lines[0].split(",");
  const idxDomain = header.indexOf("domain");
  const idxDevice = header.indexOf("device");
  const idxRun = header.indexOf("run_id");
  if (idxDomain < 0 || idxDevice < 0 || idxRun < 0) return keys;

  for (let i = 1; i < lines.length; i++) {
    const cols = parseCsvLine(lines[i]);
    const dom = cols[idxDomain] ?? "";
    const dev = cols[idxDevice] ?? "";
    const run = cols[idxRun] ?? "";
    if (dom && dev && run) keys.add(`${dom}|${dev}|${run}`);
  }
  return keys;
}

// Minimal CSV line parser for already-generated output (quotes-safe enough)
function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQ) {
      if (ch === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQ = false;
        }
      } else {
        cur += ch;
      }
    } else {
      if (ch === ",") {
        out.push(cur);
        cur = "";
      } else if (ch === '"') {
        inQ = true;
      } else {
        cur += ch;
      }
    }
  }
  out.push(cur);
  return out;
}

// ---------- Page scanning ----------
async function detectLang(page: Page): Promise<string> {
  try {
    return await page.evaluate(() => document.documentElement.lang || "");
  } catch {
    return "";
  }
}

async function detectCmpVendor(page: Page): Promise<string> {
  try {
    const scripts: string[] = await page.evaluate(() =>
      Array.from(document.scripts)
        .map((s) => s.src || "")
        .filter(Boolean)
    );
    const s = scripts.join("\n").toLowerCase();
    const vendors: [string, RegExp][] = [
      ["OneTrust", /onetrust|cookielaw/i],
      ["Cookiebot", /cookiebot/i],
      ["TrustArc", /trustarc|truste/i],
      ["Didomi", /didomi/i],
      ["Quantcast", /quantcast/i],
      ["Osano", /osano/i],
      ["Sourcepoint", /sourcepoint/i],
      ["Iubenda", /iubenda/i],
      ["Complianz", /complianz/i],
    ];
    for (const [name, re] of vendors) {
      if (re.test(s)) return name;
    }
    return "unknown";
  } catch {
    return "unknown";
  }
}

async function bannerPresentHeuristic(page: Page): Promise<number> {
  try {
    const txt: string = await page.evaluate(() => (document.body?.innerText || "").slice(0, 20000));
    return COOKIE_KEYWORDS_RE.test(txt) ? 1 : 0;
  } catch {
    return 0;
  }
}

async function findClickableByText(page: Page, re: RegExp): Promise<{ text: string } | null> {
  const loc = page
    .locator("button, a, [role='button'], input[type='button'], input[type='submit'], div[role='button']")
    .filter({ hasText: re });

  const n = await loc.count().catch(() => 0);
  if (n === 0) return null;

  for (let i = 0; i < Math.min(n, 20); i++) {
    const el = loc.nth(i);
    const vis = await el.isVisible().catch(() => false);
    if (!vis) continue;

    const text =
      (await el.innerText().catch(() => "")) ||
      (await el.getAttribute("value").catch(() => "")) ||
      "";
    return { text: text.trim() };
  }
  return null;
}


async function clickBestEffort(page: Page, re: RegExp): Promise<boolean> {
  try {
    const loc = page
      .locator("button, a, [role='button'], input[type='button'], input[type='submit'], div[role='button']")
      .filter({ hasText: re });

    if (await loc.first().isVisible({ timeout: 1200 }).catch(() => false)) {
      await loc.first().click({ timeout: 2000, force: true });
      return true;
    }
  } catch {}
  return false;
}


async function scanTogglesBestEffort(page: Page): Promise<{
  has_analytics: number;
  has_ads: number;
  default_on_analytics: number;
  default_on_ads: number;
}> {
  try {
    const res = await page.evaluate(() => {
      const items: { label: string; checked: boolean }[] = [];

      function labelText(el: Element): string {
        const aria = (el as HTMLElement).getAttribute("aria-label") || "";
        const id = (el as HTMLElement).id;
        let lbl = "";
        if (id) {
          const lab = document.querySelector(`label[for="${CSS.escape(id)}"]`);
          if (lab) lbl = (lab.textContent || "").trim();
        }
        const closestLabel = el.closest("label");
        if (!lbl && closestLabel) lbl = (closestLabel.textContent || "").trim();
        const parentText = (el.parentElement?.textContent || "").trim();
        return (aria || lbl || parentText).slice(0, 200);
      }

      // checkbox inputs
      document.querySelectorAll("input[type='checkbox']").forEach((el) => {
        const input = el as HTMLInputElement;
        items.push({ label: labelText(input), checked: input.checked });
      });

      // role switches
      document.querySelectorAll("[role='switch']").forEach((el) => {
        const v = (el as HTMLElement).getAttribute("aria-checked");
        const checked = v === "true";
        items.push({ label: labelText(el), checked });
      });

      return items;
    });

    let hasAnalytics = 0,
      hasAds = 0,
      onAnalytics = 0,
      onAds = 0;

    for (const it of res) {
      const label = (it.label || "").toLowerCase();
      if (TOGGLE_ANALYTICS_RE.test(label)) {
        hasAnalytics = 1;
        if (it.checked) onAnalytics = 1;
      }
      if (TOGGLE_ADS_RE.test(label)) {
        hasAds = 1;
        if (it.checked) onAds = 1;
      }
    }
    return {
      has_analytics: hasAnalytics,
      has_ads: hasAds,
      default_on_analytics: onAnalytics,
      default_on_ads: onAds,
    };
  } catch {
    return { has_analytics: 0, has_ads: 0, default_on_analytics: 0, default_on_ads: 0 };
  }
}

// ---------- Core runner ----------
async function runOne(browser: Browser, cand: CandidateRow, device: DeviceKind, runId: number): Promise<Obs> {
  const domain = normalizeDomain(cand.domain);
  const stratum = cand.stratum;

  const evidenceDir = path.join(ARGS.evidence, safeSlug(domain), device);
  ensureDir(evidenceDir);

  const obs: Obs = {
    // IDs
    domain,
    stratum,
    source: cand.source ?? "",
    source_rank: cand.source_rank ?? "",
    run_id: runId,
    device,
    viewport: device === "mobile" ? "390x844" : "1366x768",
    locale: ARGS.locale,
    geo_country: "TH",
    datetime_utc: nowIsoUtc(),

    // nav results
    final_url: "",
    status: "ok",
    blocked_reason: "",

    // UI presence
    banner_present: 0,
    banner_type: "", // optional; leave for manual or future heuristic
    cms_cmp_vendor: "unknown",
    language_detected: "",

    // buttons (first layer)
    accept_all_first_layer: 0,
    reject_all_first_layer: 0,
    manage_first_layer: 0,
    close_button_present: 0,

    // click depth
    clicks_accept_all: "",
    clicks_reject_all: "",
    delta_clicks: "",
    steps_reject_description: "",

    // toggles (best-effort)
    has_toggle_analytics: 0,
    has_toggle_ads: 0,
    default_on_analytics: 0,
    default_on_ads: 0,
    has_reject_all_anywhere: "",

    // network (third-party unique hosts)
    third_party_before_consent: 0,
    third_party_after_reject: "",
    tracking_after_reject_flag: "",

    // Derived DP flags (partial)
    dp1_flag: "",
    dp2_flag: "",
    dp3_flag: "",
    dp4_flag: "",
    dp5_flag: "",
    dp6_flag: "",
    dp7_flag: "",
    dp8_flag: "",

    // evidence paths
    screenshot_path_banner: "",
    screenshot_path_manage: "",
    notes: "",
  };

  const contextOptions: any = {
    locale: ARGS.locale,
    ignoreHTTPSErrors: true,
    viewport: device === "mobile" ? { width: 390, height: 844 } : { width: 1366, height: 768 },
    userAgent:
      device === "mobile"
        ? "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
        : undefined,
    isMobile: device === "mobile",
  };

  const context: BrowserContext = await browser.newContext(contextOptions);
  const page: Page = await context.newPage();

  const allReqHostsBefore = new Set<string>();
  let allReqUrlsBefore: string[] = [];

  const onRequest = (req: Request) => {
    try {
      const u = req.url();
      allReqUrlsBefore.push(u);
    } catch {}
  };

  page.on("request", onRequest);

  // Navigate with https -> fallback http if needed
  const tryUrls = cand.domain.startsWith("http")
    ? [cand.domain]
    : [`https://${domain}`, `http://${domain}`];

  let navigated = false;
  let lastErr = "";

  for (const u of tryUrls) {
    try {
      await page.goto(u, { waitUntil: "domcontentloaded", timeout: ARGS.timeoutMs });
      await page.waitForTimeout(ARGS.settleMs);
      await passInterstitials(page, obs, evidenceDir, runId, 3); // max 3 steps
      
      obs.final_url = page.url();
      navigated = true;
      break;
    } catch (e: any) {
      lastErr = String(e?.message || e).slice(0, 200);
    }
  }

  if (!navigated) {
    obs.status = "error";
    obs.blocked_reason = lastErr || "navigation failed";
    await context.close();
    return obs;
  }

  // Compute third-party before consent (unique hosts)
  // const siteHost = hostOf(obs.final_url);
  // for (const u of allReqUrlsBefore) {
  //   const h = hostOf(u);
  //   if (h && isThirdParty(h, siteHost)) allReqHostsBefore.add(domainRoot(h));
  // }
  // obs.third_party_before_consent = allReqHostsBefore.size;

  if (await detectTurnstile(page)) {
    console.log(`[TURNSTILE] ${domain} - waiting for manual solve...`);

    await page.waitForFunction(() => {
      const iframe = document.querySelector("iframe[src*='challenges.cloudflare.com']");
      return !iframe; // รอจน iframe หายไป
    }, { timeout: 120000 }); // รอสูงสุด 2 นาที

    console.log(`[TURNSTILE] solved, continuing...`);
    obs.status = "blocked_by_antibot"
    obs.blocked_reason = "cloudflare_turnstile"
    return obs;
  }

  const afterUrls: string[] = [];
  page.on("request", (req) => afterUrls.push(req.url()));

  await page.waitForTimeout(2500);

  const siteHost = hostOf(page.url());
  const tp = new Set<string>();
  for (const u of afterUrls) {
    const h = hostOf(u);
    if (h && isThirdParty(h, siteHost)) tp.add(domainRoot(h));
  }
  obs.third_party_before_consent = tp.size;

  // Basic scans
  obs.language_detected = await detectLang(page);
  obs.cms_cmp_vendor = await detectCmpVendor(page);
  obs.banner_present = await bannerPresentHeuristic(page);

  // Evidence: banner screenshot
  const bannerShot = path.join(evidenceDir, `run${runId}_banner.png`);
  try {
    await page.screenshot({ path: bannerShot, fullPage: true });
    obs.screenshot_path_banner = bannerShot;
  } catch (e: any) {
    obs.notes += `banner screenshot failed: ${String(e?.message || e).slice(0, 120)}; `;
  }

  // Detect buttons in first layer (best-effort)
  const acceptFound = await findClickableByText(page, ACCEPT_RE);
  const rejectFound = await findClickableByText(page, REJECT_RE);
  const manageFound = await findClickableByText(page, MANAGE_RE);
  const closeFound  = await findClickableByText(page, CLOSE_RE);

  obs.accept_all_first_layer = acceptFound ? 1 : 0;
  obs.reject_all_first_layer = rejectFound ? 1 : 0;
  obs.manage_first_layer     = manageFound ? 1 : 0;
  obs.close_button_present   = closeFound ? 1 : 0;


  // click depth approximations
  if (obs.accept_all_first_layer) obs.clicks_accept_all = 1;

  // click depth reject (opt-out non-essential)
  if (obs.reject_all_first_layer) {
    obs.clicks_reject_all = 1;
  } else if (obs.manage_first_layer) {
    // ใช้ทางเลือกแบบไทย: settings -> confirm choices
    const r = await rejectViaPreferences(page, obs, evidenceDir, runId);
    if (r.ok) {
      obs.clicks_reject_all = r.clickCount;
      obs.steps_reject_description = `preferences_optout:${r.details}`;
      obs.has_reject_all_anywhere = 1; // (ความหมาย: มี “ช่องทาง opt-out” ผ่าน preferences)
    } else {
      obs.clicks_reject_all = 2; // อย่างน้อยต้องเข้า manage
      obs.steps_reject_description = `preferences_failed:${r.details}`;
    }
  }

  // Try open manage panel for extra evidence + toggles + reject-all inside
  let manageOpened = false;
  if (manageFound) {
    try {
      await clickBestEffort(page, MANAGE_RE);
      await page.waitForTimeout(1500);
      manageOpened = true;

      const manageShot = path.join(evidenceDir, `run${runId}_manage.png`);
      try {
        await page.screenshot({ path: manageShot, fullPage: true });
        obs.screenshot_path_manage = manageShot;
      } catch {}

      const t = await scanTogglesBestEffort(page);
      obs.has_toggle_analytics = t.has_analytics;
      obs.has_toggle_ads = t.has_ads;
      obs.default_on_analytics = t.default_on_analytics;
      obs.default_on_ads = t.default_on_ads;
      obs.dp3_flag = (t.default_on_analytics || t.default_on_ads) ? 1 : 0;

    } catch (e: any) {
      obs.notes += `manage open failed: ${String(e?.message || e).slice(0, 120)}; `;
    }
  }

  // Determine clicks_reject_all:
  // - If direct reject in first layer: 1
  // - Else if manage exists: at least 2 (manage + reject inside), but only confirm if found
  if (obs.reject_all_first_layer) {
    obs.clicks_reject_all = 1;
  } else if (manageFound) {
    obs.clicks_reject_all = 2;
    obs.steps_reject_description = "manage->(attempt reject-all inside)";
  }

  // Derived DP1/DP2 from available click data
  if (obs.accept_all_first_layer === 1 && obs.reject_all_first_layer === 0) obs.dp1_flag = 1;
  if (obs.clicks_accept_all !== "" && obs.clicks_reject_all !== "") {
    const delta = Number(obs.clicks_reject_all) - Number(obs.clicks_accept_all);
    obs.delta_clicks = delta;
    obs.dp2_flag = delta >= 1 ? 1 : 0;
  }

  // Attempt reject-all action to measure DP7 (choice not respected)
  // Strategy:
  // 1) If reject button exists in first layer -> click it.
  // 2) Else if manage opened -> search reject-all inside and click.
  // If we can click reject, we clear request log and measure third-party hosts after action.
  let rejectClicked = false;

  // reset request collection helper for "after reject"
  const afterReqUrls: string[] = [];
  const afterListener = (req: Request) => {
    try { afterReqUrls.push(req.url()); } catch {}
  };

  page.off("request", onRequest);
  page.on("request", afterListener);

  if (rejectFound) {
    try {
      await clickBestEffort(page, REJECT_RE);
      rejectClicked = true;
    } catch {}
  } else if (manageOpened) {
    // Try find a "Reject all" inside manage panel
    try {
      const innerReject = await findClickableByText(page, REJECT_RE);
      if (innerReject) {
      await clickBestEffort(page, REJECT_RE);
        rejectClicked = true;
      } else {
        obs.steps_reject_description += " | no reject-all found (manual coding needed)";
      }
    } catch {}
  }

  if (rejectClicked) {
    obs.has_reject_all_anywhere = 1;
    try {
      await page.waitForTimeout(2500);

      const tpAfter = new Set<string>();
      for (const u of afterReqUrls) {
        const h = hostOf(u);
        if (h && isThirdParty(h, siteHost)) tpAfter.add(domainRoot(h));
      }
      obs.third_party_after_reject = tpAfter.size;
      obs.tracking_after_reject_flag = tpAfter.size > 0 ? 1 : 0;
      obs.dp7_flag = obs.tracking_after_reject_flag;

    } catch (e: any) {
      obs.notes += `after-reject measure failed: ${String(e?.message || e).slice(0, 120)}; `;
    }
  } else {
    obs.has_reject_all_anywhere = obs.reject_all_first_layer || manageOpened ? "" : 0;
  }

  await context.close();
  return obs;
}

// ---------- Main ----------
const OUTPUT_HEADER = [
  "domain","stratum","source","source_rank","run_id","device","viewport","locale","geo_country","datetime_utc",
  "final_url","status","blocked_reason",
  "banner_present","banner_type","cms_cmp_vendor","language_detected",
  "accept_all_first_layer","reject_all_first_layer","manage_first_layer","close_button_present",
  "clicks_accept_all","clicks_reject_all","delta_clicks","steps_reject_description",
  "has_toggle_analytics","has_toggle_ads","default_on_analytics","default_on_ads","has_reject_all_anywhere",
  "third_party_before_consent","third_party_after_reject","tracking_after_reject_flag",
  "dp1_flag","dp2_flag","dp3_flag","dp4_flag","dp5_flag","dp6_flag","dp7_flag","dp8_flag",
  "screenshot_path_banner","screenshot_path_manage","notes"
];

function readCandidates(filePath: string): CandidateRow[] {
  const raw0 = fs.readFileSync(filePath, "utf-8");
  // Strip UTF-8 BOM if present
  const raw = raw0.replace(/^\uFEFF/, "");

  const recs = parseCsv(raw, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    bom: true, // (csv-parse รองรับ) เผื่อมี BOM ใน header
  }) as any[];

  const out = recs
    .map((r) => {
      const domRaw = r.domain ?? r["\ufeffdomain"]; // รองรับ header มี BOM
      const domain = normalizeDomain(domRaw);
      return {
        ...r,
        domain,
        stratum: String(r.stratum ?? "").trim(),
        source: String(r.source ?? ""),
        source_rank: String(r.source_rank ?? ""),
      } as CandidateRow;
    })
    .filter((r) => r.domain.length > 0 && r.stratum.length > 0);

  const dropped = recs.length - out.length;
  if (dropped > 0) {
    console.warn(`[WARN] Dropped ${dropped} row(s) with empty domain/stratum`);
  }
  return out;
}


async function main() {
  ensureDir(ARGS.evidence);

  const candidates = readCandidates(ARGS.input);

  // Basic sanity
  const uniqDomains = new Set(candidates.map((c) => c.domain));
  console.log(`Loaded ${candidates.length} rows, unique domains: ${uniqDomains.size}`);

  // Resume keys
  const doneKeys = ARGS.resume ? loadExistingKeys(ARGS.out) : new Set<string>();
  if (ARGS.resume) console.log(`Resume enabled. Already done keys: ${doneKeys.size}`);

  const tasks: { cand: CandidateRow; device: DeviceKind; runId: number }[] = [];
  for (const cand of candidates) {
    for (const device of ["desktop","mobile"] as DeviceKind[]) {
      for (let runId = 1; runId <= ARGS.runs; runId++) {
        const key = `${normalizeDomain(cand.domain)}|${device}|${runId}`;
        if (doneKeys.has(key)) continue;
        tasks.push({ cand, device, runId });
      }
    }
  }

  console.log(`Pending tasks: ${tasks.length} (concurrency=${ARGS.concurrency}, runs=${ARGS.runs}, headful=${ARGS.headful})`);

  const browser = await chromium.launch({ headless: !ARGS.headful });
  const limit = pLimit(ARGS.concurrency);

  let done = 0;
  let failed = 0;

  const promises = tasks.map((t) =>
    limit(async () => {
      try {
        const obs = await runOne(browser, t.cand, t.device, t.runId);

        writeCsvRowAppend(ARGS.out, OUTPUT_HEADER, obs);

        done++;
        if (done % 25 === 0) console.log(`Progress: ${done}/${tasks.length} appended.`);
      } catch (e: any) {
        failed++;
        const fallback: Obs = {
          domain: normalizeDomain(t.cand.domain),
          stratum: t.cand.stratum,
          source: t.cand.source ?? "",
          source_rank: t.cand.source_rank ?? "",
          run_id: t.runId,
          device: t.device,
          viewport: t.device === "mobile" ? "390x844" : "1366x768",
          locale: ARGS.locale,
          geo_country: "TH",
          datetime_utc: nowIsoUtc(),
          final_url: "",
          status: "script_error",
          blocked_reason: String(e?.message || e).slice(0, 200),
          banner_present: 0,
          banner_type: "",
          cms_cmp_vendor: "unknown",
          language_detected: "",
          accept_all_first_layer: 0,
          reject_all_first_layer: 0,
          manage_first_layer: 0,
          close_button_present: 0,
          clicks_accept_all: "",
          clicks_reject_all: "",
          delta_clicks: "",
          steps_reject_description: "",
          has_toggle_analytics: 0,
          has_toggle_ads: 0,
          default_on_analytics: 0,
          default_on_ads: 0,
          has_reject_all_anywhere: "",
          third_party_before_consent: 0,
          third_party_after_reject: "",
          tracking_after_reject_flag: "",
          dp1_flag: "",
          dp2_flag: "",
          dp3_flag: "",
          dp4_flag: "",
          dp5_flag: "",
          dp6_flag: "",
          dp7_flag: "",
          dp8_flag: "",
          screenshot_path_banner: "",
          screenshot_path_manage: "",
          notes: "Unhandled script error",
        };
        writeCsvRowAppend(ARGS.out, OUTPUT_HEADER, fallback);
      }
    })
  );

  await Promise.all(promises);
  await browser.close();

  console.log(`Done. appended=${done}, failed=${failed}`);
  console.log(`Output: ${ARGS.out}`);
  console.log(`Evidence dir: ${ARGS.evidence}`);
}

main().catch((e) => {
  console.error("Fatal:", e);
  process.exit(1);
});


async function clickByRegex(
  page: Page,
  include: RegExp,
  opts?: { exclude?: RegExp; maxScan?: number; force?: boolean }
): Promise<{ clicked: boolean; text: string }> {
  const exclude = opts?.exclude;
  const maxScan = opts?.maxScan ?? 30;

  const loc = page.locator(
    "button, a, [role='button'], input[type='button'], input[type='submit'], div[role='button']"
  );

  const n = await loc.count().catch(() => 0);
  for (let i = 0; i < Math.min(n, maxScan); i++) {
    const el = loc.nth(i);
    const vis = await el.isVisible().catch(() => false);
    if (!vis) continue;

    const txt =
      (await el.innerText().catch(() => "")) ||
      (await el.getAttribute("value").catch(() => "")) ||
      "";
    const t = txt.trim();

    if (!t) continue;
    if (!include.test(t)) continue;
    if (exclude && exclude.test(t)) continue;

    try {
      await el.click({ timeout: 2000, force: opts?.force ?? true });
      return { clicked: true, text: t };
    } catch {
      // continue scan
    }
  }

  return { clicked: false, text: "" };
}


async function tickVisibleCheckboxesBestEffort(page: Page, maxTicks = 3): Promise<number> {
  let ticks = 0;

  // input checkbox
  const cb = page.locator("input[type='checkbox']");
  const n1 = await cb.count().catch(() => 0);
  for (let i = 0; i < Math.min(n1, 20) && ticks < maxTicks; i++) {
    const el = cb.nth(i);
    const vis = await el.isVisible().catch(() => false);
    if (!vis) continue;

    const checked = await el.isChecked().catch(() => false);
    if (!checked) {
      try {
        await el.check({ timeout: 1500, force: true });
        ticks++;
      } catch {
        try {
          await el.click({ timeout: 1500, force: true });
          ticks++;
        } catch {}
      }
    }
  }

  // role switch (บางเว็บใช้ switch แทน checkbox)
  const sw = page.locator("[role='checkbox'], [role='switch']");
  const n2 = await sw.count().catch(() => 0);
  for (let i = 0; i < Math.min(n2, 20) && ticks < maxTicks; i++) {
    const el = sw.nth(i);
    const vis = await el.isVisible().catch(() => false);
    if (!vis) continue;

    const aria = (await el.getAttribute("aria-checked").catch(() => "")) || "";
    if (aria === "false") {
      try {
        await el.click({ timeout: 1500, force: true });
        ticks++;
      } catch {}
    }
  }

  return ticks;
}

async function passInterstitials(
  page: Page,
  obs: Record<string, any>,
  evidenceDir: string,
  runId: number,
  maxSteps = 3
): Promise<void> {
  for (let step = 1; step <= maxSteps; step++) {
    // screenshot ก่อนพยายามกด
    const shotBefore = path.join(evidenceDir, `run${runId}_interstitial_step${step}_before.png`);
    await page.screenshot({ path: shotBefore, fullPage: true }).catch(() => {});
    obs.notes = (obs.notes || "") + `interstitial_step${step}_before=${shotBefore}; `;

    // ลองกด "เข้าเว็บไซต์/continue" (กัน cookie)
    const click1 = await clickByRegex(page, ENTER_SITE_RE, { exclude: COOKIE_WORD_RE, maxScan: 40, force: true });

    if (!click1.clicked) {
      // ถ้าไม่เจอปุ่มเลย → จบ
      break;
    }

    obs.notes = (obs.notes || "") + `interstitial_clicked="${click1.text}"; `;

    // รอ navigation แบบ best-effort (บางเว็บเป็น SPA ไม่เปลี่ยน URL)
    await Promise.race([
      page.waitForNavigation({ timeout: 8000, waitUntil: "domcontentloaded" }).catch(() => null),
      page.waitForLoadState("domcontentloaded", { timeout: 8000 }).catch(() => null),
      page.waitForTimeout(2000),
    ]);

    // ถ้าปุ่มถูก disable เพราะต้องติ๊ก checkbox → ติ๊กแล้วกดอีกครั้ง
    // (กรณีแรก click ไปแล้วแต่ยังอยู่หน้าเดิม)
    const urlAfter = page.url();
    const shotAfter = path.join(evidenceDir, `run${runId}_interstitial_step${step}_after.png`);
    await page.screenshot({ path: shotAfter, fullPage: true }).catch(() => {});
    obs.notes = (obs.notes || "") + `interstitial_step${step}_after=${shotAfter}; `;

    // heuristic: ถ้ายังมีคำว่า "เข้าเว็บไซต์" อยู่ หรือยังหน้าเดิมมาก ๆ ให้ลองติ๊ก checkbox แล้วกดอีกครั้ง
    const stillInterstitial = await page
      .locator("body")
      .innerText()
      .then((t) => ENTER_SITE_RE.test((t || "").slice(0, 20000)))
      .catch(() => false);

    if (stillInterstitial) {
      const ticks = await tickVisibleCheckboxesBestEffort(page, 2);
      if (ticks > 0) {
        obs.notes = (obs.notes || "") + `interstitial_checkboxes_ticked=${ticks}; `;
        const click2 = await clickByRegex(page, ENTER_SITE_RE, { exclude: COOKIE_WORD_RE, maxScan: 40, force: true });
        if (click2.clicked) {
          await Promise.race([
            page.waitForNavigation({ timeout: 8000, waitUntil: "domcontentloaded" }).catch(() => null),
            page.waitForLoadState("domcontentloaded", { timeout: 8000 }).catch(() => null),
            page.waitForTimeout(2000),
          ]);
        }
      }
    }

    // ถ้า step นี้ผ่านแล้ว ก็วนอีกครั้ง เผื่อมี interstitial ซ้อน
    // แต่ถ้าไม่เหลือข้อความ enter-site แล้วให้หยุดเร็ว
    const still2 = await page
      .locator("body")
      .innerText()
      .then((t) => ENTER_SITE_RE.test((t || "").slice(0, 20000)))
      .catch(() => false);

    if (!still2) break;
  }
}

async function getToggleLabel(el: Locator): Promise<string> {
  // ใช้ evaluate เล็ก ๆ เฉพาะ element (ปลอดภัยกว่า evaluateHandle ใหญ่ ๆ)
  return await el
    .evaluate((node) => {
      const e = node as HTMLElement;
      const aria = e.getAttribute("aria-label") || "";
      const id = (e as any).id || "";
      let lbl = "";

      if (id) {
        const lab = document.querySelector(`label[for="${CSS.escape(id)}"]`);
        if (lab) lbl = (lab.textContent || "").trim();
      }

      if (!lbl) {
        const closestLabel = e.closest("label");
        if (closestLabel) lbl = (closestLabel.textContent || "").trim();
      }

      const parentText = (e.parentElement?.textContent || "").trim();
      return (aria || lbl || parentText).slice(0, 200);
    })
    .catch(() => "");
}

async function toggleOffNonEssentialBestEffort(page: Page): Promise<{
  has_analytics: number;
  has_ads: number;
  default_on_analytics: number;
  default_on_ads: number;
  toggles_clicked: number;
}> {
  let hasAnalytics = 0, hasAds = 0;
  let onAnalytics = 0, onAds = 0;
  let togglesClicked = 0;

  // 1) checkbox inputs
  const cbs = page.locator("input[type='checkbox']");
  const nCb = await cbs.count().catch(() => 0);
  for (let i = 0; i < Math.min(nCb, 30); i++) {
    const el = cbs.nth(i);
    if (!(await el.isVisible().catch(() => false))) continue;

    const label = (await getToggleLabel(el)).toLowerCase();
    const isAnalytics = TOGGLE_ANALYTICS_RE.test(label);
    const isAds = TOGGLE_ADS_RE.test(label);
    if (!isAnalytics && !isAds) continue;

    const checked = await el.isChecked().catch(() => false);
    if (isAnalytics) { hasAnalytics = 1; if (checked) onAnalytics = 1; }
    if (isAds) { hasAds = 1; if (checked) onAds = 1; }

    if (checked) {
      // ปิด
      await el.click({ timeout: 1500, force: true }).catch(() => {});
      togglesClicked++;
    }
  }

  // 2) role switches
  const sw = page.locator("[role='switch'], [role='checkbox']");
  const nSw = await sw.count().catch(() => 0);
  for (let i = 0; i < Math.min(nSw, 30); i++) {
    const el = sw.nth(i);
    if (!(await el.isVisible().catch(() => false))) continue;

    const label = (await getToggleLabel(el)).toLowerCase();
    const isAnalytics = TOGGLE_ANALYTICS_RE.test(label);
    const isAds = TOGGLE_ADS_RE.test(label);
    if (!isAnalytics && !isAds) continue;

    const aria = (await el.getAttribute("aria-checked").catch(() => "")) || "";
    const checked = aria === "true";

    if (isAnalytics) { hasAnalytics = 1; if (checked) onAnalytics = 1; }
    if (isAds) { hasAds = 1; if (checked) onAds = 1; }

    if (checked) {
      await el.click({ timeout: 1500, force: true }).catch(() => {});
      togglesClicked++;
    }
  }

  return {
    has_analytics: hasAnalytics,
    has_ads: hasAds,
    default_on_analytics: onAnalytics,
    default_on_ads: onAds,
    toggles_clicked: togglesClicked,
  };
}


async function rejectViaPreferences(
  page: Page,
  obs: Record<string, any>,
  evidenceDir: string,
  runId: number
): Promise<{ ok: boolean; clickCount: number; details: string }> {
  // 1) เปิดหน้า settings/manage
  const opened = await clickBestEffort(page, MANAGE_RE);
  if (!opened) return { ok: false, clickCount: 0, details: "manage_not_found" };

  await page.waitForTimeout(1200);

  // screenshot manage
  const manageShot = path.join(evidenceDir, `run${runId}_manage.png`);
  await page.screenshot({ path: manageShot, fullPage: true }).catch(() => {});
  obs.screenshot_path_manage = manageShot;

  // 2) ตรวจ/ปิด toggle analytics & ads (best-effort)
  const t = await toggleOffNonEssentialBestEffort(page);
  obs.has_toggle_analytics = t.has_analytics;
  obs.has_toggle_ads = t.has_ads;
  obs.default_on_analytics = t.default_on_analytics;
  obs.default_on_ads = t.default_on_ads;
  obs.dp3_flag = (t.default_on_analytics || t.default_on_ads) ? 1 : obs.dp3_flag;

  // 3) กดปุ่มยืนยันตัวเลือกของฉัน/Save settings
  // กันไม่ให้ไปกด "ยอมรับทั้งหมด" ใน modal
  const clicked = await clickByRegex(page, CONFIRM_PREFS_RE, { exclude: ACCEPT_ALL_RE, maxScan: 40, force: true });
  if (!clicked.clicked) {
    // fallback: บางเว็บใช้คำว่า "ปิด" หรือ "ยืนยัน" เฉย ๆ
    const clicked2 = await clickByRegex(page, /(ยืนยัน|บันทึก|ตกลง|confirm|save)/i, { exclude: ACCEPT_ALL_RE, maxScan: 40, force: true });
    if (!clicked2.clicked) return { ok: false, clickCount: 1, details: "confirm_not_found" };
  }

  await page.waitForTimeout(2500);

  // clickCount = 1 (manage) + togglesClicked + 1 (confirm)
  const clickCount = 1 + t.toggles_clicked + 1;
  return { ok: true, clickCount, details: `manage+${t.toggles_clicked}toggles+confirm` };
}

async function detectTurnstile(page: Page): Promise<boolean> {
  try {
    const hasIframe = await page
      .locator("iframe[src*='challenges.cloudflare.com']")
      .count();

    const bodyText = await page.locator("body").innerText().catch(() => "");
    const textFlag = /cloudflare|verify you are human|ตรวจสอบว่าคุณเป็นมนุษย์/i.test(bodyText);

    return hasIframe > 0 || textFlag;
  } catch {
    return false;
  }
}