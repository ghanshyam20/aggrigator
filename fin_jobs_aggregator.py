#!/usr/bin/env python3
# Finnish Job Aggregator — multi-site + pagination + per-page default_location
# Built-in filters for HEL/ESPOO/VANTAA + Warehouse/Kitchen/Cleaning/Fast-food keywords.

from __future__ import annotations
import argparse, csv, dataclasses, datetime as dt, json, re, time
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import yaml
import dateparser

# -------- Built-in filters (applied by default) --------
DEFAULT_LOCATIONS = ["Helsinki", "Espoo", "Vantaa"]
DEFAULT_KEYWORDS = [
    # Warehouse / logistics (EN + FI)
    "warehouse","warehousing","logistics","logistic","picker","packer","forklift","varasto","logistiikka",
    "material handler","order picker","post sorter",
    # Cleaning (EN + FI)
    "cleaner","cleaning","janitor","housekeeping","siivous","siivooja","toimitilahuoltaja",
    # Kitchen / restaurant (EN + FI)
    "kitchen","dishwasher","cook","chef","ravintola","keittiö","keittio","keittiöapulainen",
    "astiahuoltaja","tiskaaja","line cook",
    # Fast food brands common in Finland
    "fast food","pikaruoka","McDonalds","Hesburger","Burger King","Subway","Taco Bell","Kotipizza"
]

# -------- HTTP defaults --------
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "en,fi;q=0.8",
}

@dataclasses.dataclass
class Job:
    source: str
    title: str
    company: str
    location: str
    url: str
    posted: Optional[str] = None
    posted_dt: Optional[str] = None
    salary: Optional[str] = None
    snippet: Optional[str] = None
    def key(self) -> str: return self.url.strip().lower()

# ---------- helpers ----------
def normalize_space(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def parse_date(text: str) -> Optional[str]:
    if not text: return None
    d = dateparser.parse(text, languages=["fi","en"], settings={"DATE_ORDER":"DMY"})
    return d.date().isoformat() if d else None

def fetch_url(url: str, timeout: int = 25) -> Optional[str]:
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
        if r.status_code == 200:
            return r.text
        return None
    except Exception:
        return None

def sel_text(node, sel: Optional[str]) -> str:
    if not sel: return ""
    el = node.select_one(sel)
    return normalize_space(el.get_text(" ") if el else "")

def sel_attr(node, sel: Optional[str], attr: str) -> str:
    if not sel: return ""
    el = node.select_one(sel)
    return el.get(attr) if el else ""

def csv_list(s: str) -> List[str]:
    return [normalize_space(x) for x in s.split(",") if normalize_space(x)]

# ---------- extraction ----------
def extract_jobs_from_html(
    html: str,
    rules: Dict[str, Any],
    source_name: str,
    default_location: Optional[str] = None
) -> List[Job]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Job] = []

    selectors = rules.get("selectors", rules)
    container_sel = selectors.get("container")

    # Card-based (if we have selectors)
    if container_sel:
        for card in soup.select(container_sel):
            title = sel_text(card, selectors.get("title"))
            company = sel_text(card, selectors.get("company"))
            location = sel_text(card, selectors.get("location")) or (default_location or "")
            url = sel_attr(card, selectors.get("link"), "href")
            if url:
                url = urljoin(selectors.get("base",""), url)
            posted_txt = sel_text(card, selectors.get("posted"))
            salary = sel_text(card, selectors.get("salary"))
            snippet = sel_text(card, selectors.get("snippet"))
            if title and url:
                out.append(Job(
                    source=source_name, title=title, company=company, location=location,
                    url=url, posted=posted_txt, posted_dt=parse_date(posted_txt) if posted_txt else None,
                    salary=salary, snippet=snippet
                ))

    # Fallback: regex link harvesting (works even if site changes)
    link_pat = selectors.get("link_pattern")
    if link_pat:
        pat = re.compile(link_pat)
        base = selectors.get("base","")
        for a in soup.select("a[href]"):
            href = a.get("href","")
            if not href or not pat.search(href): continue
            url = urljoin(base, href)
            title = normalize_space(a.get_text(" ")) or "Job posting"
            # Avoid duplication if captured above
            if not any(j.url == url for j in out):
                out.append(Job(
                    source=source_name, title=title, company="",
                    location=default_location or "", url=url,
                    posted=None, posted_dt=None, salary=None, snippet=""
                ))

    return out

def find_next_url(soup: BeautifulSoup, selectors: Dict[str, Any]) -> Optional[str]:
    base = selectors.get("base","")
    # Explicit CSS selector first
    next_sel = selectors.get("next_selector")
    if next_sel:
        el = soup.select_one(next_sel)
        if el and el.get("href"):
            return urljoin(base, el.get("href"))
    # rel=next
    el = soup.select_one('a[rel="next"]')
    if el and el.get("href"):
        return urljoin(base, el.get("href"))
    # Text heuristics
    for txt in ("Seuraava", "Next", "Näytä lisää", "Load more"):
        el = soup.find("a", string=re.compile(txt, re.I))
        if el and el.get("href"):
            return urljoin(base, el.get("href"))
    return None

# ---------- crawling ----------
def build_pages(cfg: Dict[str, Any]) -> List[Tuple[str, Optional[str]]]:
    specs: List[Tuple[str, Optional[str]]] = []
    if "pages" in cfg:
        for p in cfg["pages"]:
            if isinstance(p, dict):
                specs.append((p["url"], p.get("default_location")))
            else:
                specs.append((str(p), cfg.get("default_location")))
    elif "urls" in cfg:
        for u in cfg["urls"]:
            specs.append((u, cfg.get("default_location")))
    elif "url" in cfg:
        specs.append((cfg["url"], cfg.get("default_location")))
    return specs

def crawl_site(name: str, cfg: Dict[str, Any], max_items: int) -> List[Job]:
    results: List[Job] = []
    seen: set[str] = set()
    delay = float(cfg.get("delay", 0.8))
    selectors = cfg.get("selectors", cfg)
    max_pages = int(cfg.get("max_pages", 4))  # follow up to 4 pages per start URL

    for start_url, default_loc in build_pages(cfg):
        url = start_url
        pages_seen = 0
        visited = set()
        while url and pages_seen < max_pages and len(results) < max_items:
            if url in visited: break
            visited.add(url)

            html = fetch_url(url)
            if not html: break

            soup = BeautifulSoup(html, "html.parser")
            jobs = extract_jobs_from_html(html, cfg, name, default_location=default_loc)
            for j in jobs:
                k = j.key()
                if k in seen: continue
                seen.add(k); results.append(j)
                if len(results) >= max_items: break

            pages_seen += 1
            if len(results) >= max_items: break

            next_url = find_next_url(soup, selectors)
            url = next_url
            if url: time.sleep(delay)

    return results

# ---------- outputs ----------
def save_csv(path: str, jobs: List[Job]) -> None:
    fields = ["source","title","company","location","url","posted","posted_dt","salary","snippet"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for j in jobs: w.writerow(dataclasses.asdict(j))

def save_json(path: str, jobs: List[Job]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump([dataclasses.asdict(j) for j in jobs], f, ensure_ascii=False, indent=2)

HTML_TEMPLATE = r"""<!doctype html>
<html lang='en'>
<head>
<meta charset='utf-8'/>
<meta name='viewport' content='width=device-width,initial-scale=1'/>
<title>Jobs — aggregated</title>
<style>
  :root { --bg:#0b0d10; --card:#13161a; --muted:#9aa4af; --text:#e8eef5; --accent:#5fa8ff; }
  * { box-sizing: border-box; font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, Arial, 'Apple Color Emoji', 'Segoe UI Emoji'; }
  body { margin: 0; background: var(--bg); color: var(--text); }
  header { position: sticky; top: 0; z-index: 10; background: rgba(11,13,16,0.8); backdrop-filter: blur(8px); padding: 16px; border-bottom: 1px solid #1f242b; }
  .wrap { max-width: 1100px; margin: 0 auto; padding: 16px; }
  h1 { margin: 0 0 12px; font-size: 22px; }
  .controls { display: grid; grid-template-columns: 1fr 220px 220px; gap: 10px; }
  input, select { width: 100%; padding: 10px 12px; background: #0f1216; color: var(--text); border: 1px solid #232a32; border-radius: 10px; }
  main.wrap { display: grid; grid-template-columns: 1fr; gap: 12px; }
  .card { background: var(--card); border: 1px solid #1f242b; border-radius: 14px; padding: 14px; }
  .card-head { display:flex; justify-content: space-between; align-items: baseline; gap: 10px; }
  .title { color: var(--text); text-decoration: none; font-weight: 650; }
  .title:hover { color: var(--accent); }
  .source { font-size: 12px; color: var(--muted); }
  .meta { margin-top: 6px; font-size: 13px; color: var(--muted); }
  .sep { margin: 0 6px; opacity: .6; }
  .snippet { margin: 8px 0 10px; color: #c9d2db; font-size: 14px; }
  .actions { display:flex; gap:10px; }
  .actions .apply, .actions button { appearance:none; border:1px solid #2a313a; background:#0f1216; color:var(--text); padding:8px 12px; border-radius:10px; text-decoration:none; font-size:14px; }
  .actions .apply:hover, .actions button:hover { border-color:#39424d; }
  footer { color: var(--muted); text-align:center; padding: 18px; font-size: 12px; }
  .grid { display:grid; gap:12px; }
  .count { font-size: 13px; color: var(--muted); margin-top:8px; }
  .chips { display:flex; flex-wrap:wrap; gap:8px; margin-top:8px; }
  .chip { font-size:12px; padding:4px 8px; border:1px solid #26303a; border-radius:999px; color:#cbd5e1; background:#0f1216; }
</style>
</head>
<body>
  <header>
    <div class='wrap'>
      <h1>Aggregated Jobs</h1>
      <div class='controls'>
        <input id='q' placeholder='Filter by title, company, etc.'/>
        <select id='source'><option value=''>All sources</option></select>
        <input id='loc' placeholder='Filter by location (e.g., Helsinki)'/>
      </div>
      <div class='chips' id='active'></div>
      <div class='count' id='count'></div>
    </div>
  </header>
  <main class='wrap grid' id='list'>
    __ROWS__
  </main>
  <footer>Generated on __TODAY__ — locally saved file. Click a card to apply ↗</footer>
<script>
(function(){
  const list = document.getElementById('list');
  const q = document.getElementById('q');
  const sourceSel = document.getElementById('source');
  const loc = document.getElementById('loc');
  const cards = Array.from(list.querySelectorAll('.card'));
  const count = document.getElementById('count');
  const active = document.getElementById('active');

  // Show active filters from data attributes injected by Python
  (function(){
    const kws = (document.body.dataset.keywords || '').split('|').filter(Boolean);
    const locs = (document.body.dataset.locations || '').split('|').filter(Boolean);
    const add = (txt) => { const s=document.createElement('span'); s.className='chip'; s.textContent=txt; active.appendChild(s); };
    if (kws.length) { add('Keywords:'); kws.forEach(add); }
    if (locs.length) { add('Locations:'); locs.forEach(add); }
  })();

  // Build source dropdown
  const sources = Array.from(new Set(cards.map(c => c.dataset.source))).sort();
  sources.forEach(s => { const opt = document.createElement('option'); opt.value = s; opt.textContent = s; sourceSel.appendChild(opt); });

  function applyFilters(){
    const term = q.value.trim().toLowerCase();
    const src = sourceSel.value;
    const locTerm = loc.value.trim().toLowerCase();
    let visible = 0;
    cards.forEach(c => {
      const text = c.dataset.text.toLowerCase();
      const okTerm = !term || text.includes(term);
      const okSrc = !src || c.dataset.source === src;
      const okLoc = !locTerm || (c.dataset.location || '').toLowerCase().includes(locTerm);
      const show = okTerm && okSrc && okLoc;
      c.style.display = show ? '' : 'none';
      if (show) visible++;
    });
    count.textContent = visible + ' jobs shown';
  }

  q.addEventListener('input', applyFilters);
  sourceSel.addEventListener('change', applyFilters);
  loc.addEventListener('input', applyFilters);
  applyFilters();
})();
</script>
</body>
</html>
"""

def esc_html(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;").replace("'","&#39;")

def build_html(jobs: List[Job], path: str, keywords: List[str], locations: List[str]) -> None:
    rows = []
    for j in jobs:
        title, company, location = esc_html(j.title), esc_html(j.company), esc_html(j.location)
        snippet = esc_html(j.snippet or ""); posted = esc_html(j.posted_dt or j.posted or "")
        source = esc_html(j.source); url = j.url
        data_text = esc_html(f"{title} {company} {location} {snippet} {source}")
        rows.append(
            "<article class='card' data-source='{src}' data-location='{loc}' data-text='{dt}'>"
            "<div class='card-head'>"
            "<a class='title' href='{url}' target='_blank' rel='noopener'>{title}</a>"
            "<span class='source'>{src}</span>"
            "</div>"
            "<div class='meta'><span class='company'>{company}</span><span class='sep'>•</span>"
            "<span class='location'>{loc}</span><span class='sep'>•</span><span class='posted'>{posted}</span></div>"
            "<p class='snippet'>{snippet}</p>"
            "<div class='actions'><button onclick=\"navigator.clipboard.writeText('{url}')\">Copy link</button>"
            "<a class='apply' href='{url}' target='_blank' rel='noopener'>Open & Apply ↗</a></div>"
            "</article>"
        .format(src=source, loc=location, dt=data_text, url=url, title=title, company=company, posted=posted, snippet=snippet))

    html = HTML_TEMPLATE.replace("__ROWS__", "\n".join(rows)).replace("__TODAY__", dt.date.today().isoformat())
    # Inject active filters as data attributes on <body>
    body_tag = "<body>"
    body_tag_with_data = f"<body data-keywords=\"{'|'.join(keywords)}\" data-locations=\"{'|'.join(locations)}\">"
    html = html.replace(body_tag, body_tag_with_data, 1)
    with open(path, "w", encoding="utf-8") as f: f.write(html)

# ---------- filtering ----------
def filter_jobs(jobs: List[Job], keywords: List[str], locations: List[str]) -> List[Job]:
    if not keywords and not locations: return jobs
    def ok(j: Job) -> bool:
        text = " ".join([j.title.lower(), j.company.lower(), j.location.lower(), (j.snippet or "").lower()])
        kw_ok = True if not keywords else any(k.lower() in text for k in keywords)
        loc_ok = True if not locations else any(l.lower() in text for l in locations)
        return kw_ok and loc_ok
    return [j for j in jobs if ok(j)]

# ---------- YAML / CLI ----------
def load_sites(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f: return yaml.safe_load(f) or {}

def main():
    ap = argparse.ArgumentParser(description="Finnish job aggregator (multi-site, configurable with YAML)")
    ap.add_argument("--sites", required=True, help="Path to sites.yaml with scraping rules")
    ap.add_argument("--keywords", default="", help="Comma-separated keywords to include (default: built-in)")
    ap.add_argument("--locations", default="", help="Comma-separated locations to include (default: HEL/Espoo/Vantaa)")
    ap.add_argument("--no-filters", action="store_true", help="Disable all keyword/location filtering")
    ap.add_argument("--max-per-site", type=int, default=120)
    ap.add_argument("--out-csv", default="jobs.csv")
    ap.add_argument("--out-json", default="jobs.json")
    ap.add_argument("--out-html", default="jobs.html")
    ap.add_argument("--out-links", default="links.txt", help="Plain list of URLs")
    ap.add_argument("--days", type=int, default=120, help="Keep only jobs posted within N days if dates are parsed")
    args = ap.parse_args()

    # Decide filters
    if args.no_filters:
        keywords, locations = [], []
    else:
        keywords = csv_list(args.keywords) if args.keywords else DEFAULT_KEYWORDS
        locations = csv_list(args.locations) if args.locations else DEFAULT_LOCATIONS

    # Load sites
    sites_cfg = load_sites(args.sites)

    # Crawl all
    all_jobs: List[Job] = []
    for name, cfg in sites_cfg.items():
        print(f"[+] Crawling {name}…")
        try:
            jobs = crawl_site(name, cfg, args.max_per_site)
        except Exception as e:
            print(f"[-] {name} failed: {e}")
            continue
        all_jobs.extend(jobs)

    # Deduplicate
    dedup: Dict[str, Job] = {}
    for j in all_jobs: dedup[j.key()] = j
    jobs = list(dedup.values())

    # Date filter
    if args.days and any(j.posted_dt for j in jobs):
        cutoff = (dt.date.today() - dt.timedelta(days=args.days)).isoformat()
        jobs = [j for j in jobs if not j.posted_dt or j.posted_dt >= cutoff]

    # Keyword/location filter
    jobs = filter_jobs(jobs, keywords, locations)

    # Sort
    jobs.sort(key=lambda j: (j.posted_dt or "0000-00-00", j.source), reverse=True)

    # Save outputs
    save_csv(args.out_csv, jobs)
    save_json(args.out_json, jobs)
    build_html(jobs, args.out_html, keywords, locations)
    with open(args.out_links, "w", encoding="utf-8") as f:
        for j in jobs: f.write(j.url + "\n")

    print(f"Saved {len(jobs)} jobs → {args.out_csv}, {args.out_json}, {args.out_html}, {args.out_links}")

if __name__ == "__main__":
    main()
