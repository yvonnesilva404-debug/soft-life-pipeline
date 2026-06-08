"""
enrich.py — Second-pass pay enrichment for active job records.

Fetches full JD + pay from each job's URL (ATS dispatch → JSON-LD → DOM → description regex).
Runs BEFORE the accept/reject split in pipeline.py so enriched-pay jobs can escape reject.csv.

Fixed imports (was: core.config, core.db, scripts.pull_descriptions_from_harvesters).

Public API:
    enrich_apply_tier(jobs: list[dict], verbose: bool = True) -> list[dict]
"""

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

import fetch as _fetch
from Pay import extract_pay as _pay_extract

# ---------------------------------------------------------------------------
# Config (was: from core.config import HARVEST_DEFAULTS)
# ---------------------------------------------------------------------------

_HARVEST_DEFAULTS = {
    "DETAIL_ENRICH_WORKERS": 4,
    "DETAIL_ENRICH_TIMEOUT": 12.0,
}

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

APPLY_SELECTORS = [
    'a[href*="apply"]', 'a[data-testid*="apply"]', 'a[class*="apply"]',
    'a[aria-label*="pply"]', "a#apply_button", ".postings-btn-wrapper a",
    "a.ashby-job-posting-apply-button", '#grnhse_app a[href*="apply"]',
    'a[data-qa="btn-apply"]', 'a[class*="btn-apply"]',
]

DESCRIPTION_SELECTORS = [
    "#job-description", "#job_description", "#jobDescriptionText",
    ".job-description", ".job_description", '[class*="job-description"]',
    '[class*="jobDescription"]', '[data-testid*="description"]',
    '[data-testid="job-description"]', ".ashby-job-posting-description",
    ".job-posting-content", ".html-parsed-content", "[id^='job-post-body-']",
    '[role="main"] article', "main article", "article",
]

PAY_PATTERNS = [
    re.compile(
        r"(\$?\d[\d,]*(?:\.\d{2})?\s*(?:-|to)\s*\$?\d[\d,]*(?:\.\d{2})?\s*"
        r"(?:per\s+year|per\s+hour|a\s+year|a\s+hour|annually|hourly|/year|/hour|/hr)?)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(\$?\d[\d,]*(?:\.\d{2})?\s*"
        r"(?:per\s+year|per\s+hour|a\s+year|a\s+hour|annually|hourly|/year|/hour|/hr))",
        re.IGNORECASE,
    ),
]

EMPLOYMENT_TYPE_PATTERNS = [
    ("Full-time", re.compile(r"\bfull[-\s]?time\b", re.IGNORECASE)),
    ("Part-time", re.compile(r"\bpart[-\s]?time\b", re.IGNORECASE)),
    ("Contract", re.compile(r"\b(contract|contractor)\b", re.IGNORECASE)),
    ("Internship", re.compile(r"\b(internship|intern)\b", re.IGNORECASE)),
    ("Temporary", re.compile(r"\btemporary\b", re.IGNORECASE)),
]


# ── Text cleaning ─────────────────────────────────────────────────────────────

def _clean(text: str) -> str:
    if not text:
        return ""
    raw = str(text)
    if "<" in raw and ">" in raw:
        soup = BeautifulSoup(raw, "html.parser")
        for br in soup.find_all("br"):
            br.replace_with("\n")
        for tag in soup.find_all(["p", "div", "h1", "h2", "h3", "h4", "li", "tr"]):
            tag.insert_before("\n")
            tag.insert_after("\n")
        for li in soup.find_all("li"):
            li.insert_before("- ")
        raw = soup.get_text()
    lines = [l.strip() for l in raw.split("\n") if l.strip()]
    cleaned = "\n".join(lines)
    return re.sub(r"\n{3,}", "\n\n", cleaned).strip()


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


# ── Extraction helpers ────────────────────────────────────────────────────────

def _jsonld_salary_to_text(posting: Dict) -> str:
    salary = posting.get("baseSalary") or posting.get("estimatedSalary") or {}
    if isinstance(salary, list) and salary:
        salary = salary[0]
    if not isinstance(salary, dict):
        return ""

    currency = str(salary.get("currency") or "USD").strip()
    symbol = "$" if currency.upper() == "USD" else f"{currency} "

    value = salary.get("value") or {}
    if isinstance(value, list) and value:
        value = value[0]
    if not isinstance(value, dict):
        value = {}

    min_value = value.get("minValue") or value.get("value")
    max_value = value.get("maxValue")
    unit = str(value.get("unitText") or salary.get("unitText") or "").lower()

    if unit in {"year", "yearly"}:
        unit_text = "per year"
    elif unit in {"hour", "hourly"}:
        unit_text = "per hour"
    else:
        unit_text = ""

    if min_value and max_value:
        return _normalize_space(f"{symbol}{min_value} - {symbol}{max_value} {unit_text}")
    if min_value:
        return _normalize_space(f"{symbol}{min_value} {unit_text}")
    return ""


def _json_ld(soup: BeautifulSoup, page_url: str) -> Optional[Dict]:
    for el in soup.select('script[type="application/ld+json"]'):
        raw = el.get_text(strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        posting = _find_posting(data)
        if not posting:
            continue
        desc = _clean(str(posting.get("description") or ""))
        if len(desc) < 80:
            continue
        apply_url = posting.get("url") or posting.get("directApply")
        if isinstance(apply_url, str):
            apply_url = urljoin(page_url, apply_url.strip())
        pay = _jsonld_salary_to_text(posting)
        employment_type = posting.get("employmentType")
        if isinstance(employment_type, list):
            employment_type = ", ".join(str(item) for item in employment_type if item)
        return {
            "description": desc,
            "apply_url": apply_url,
            "pay": _normalize_space(pay),
            "employment_type": _normalize_space(str(employment_type or "")),
        }
    return None


def _find_posting(data):
    if isinstance(data, dict):
        if str(data.get("@type", "")).lower() == "jobposting":
            return data
        graph = data.get("@graph")
        if isinstance(graph, list):
            for item in graph:
                r = _find_posting(item)
                if r:
                    return r
    elif isinstance(data, list):
        for item in data:
            r = _find_posting(item)
            if r:
                return r
    return None


def _extract_apply_url(soup: BeautifulSoup, page_url: str) -> Optional[str]:
    for sel in APPLY_SELECTORS:
        el = soup.select_one(sel)
        if not el:
            continue
        href = el.get("href")
        if isinstance(href, str) and href.strip() and href.strip() != "#":
            return urljoin(page_url, href.strip())
    for link in soup.find_all("a", href=True):
        text = link.get_text(" ", strip=True).lower()
        href = str(link.get("href", "")).strip()
        if "apply" in text and href and href != "#" and "javascript:" not in href.lower():
            return urljoin(page_url, href)
    return None


def _extract_description(soup: BeautifulSoup) -> str:
    for selector in DESCRIPTION_SELECTORS:
        node = soup.select_one(selector)
        if not node:
            continue
        text = _clean(node.get_text("\n", strip=True))
        if len(text) >= 80:
            return text
    return ""


def _extract_pay_text(soup: BeautifulSoup) -> str:
    text = _normalize_space(soup.get_text(" ", strip=True))
    if not text:
        return ""
    for pattern in PAY_PATTERNS:
        match = pattern.search(text)
        if match:
            normalized = _pay_extract(match.group(1))
            return normalized or ""
    return ""


def _extract_employment_type(soup: BeautifulSoup) -> str:
    text = _normalize_space(soup.get_text(" ", strip=True))
    if not text:
        return ""
    for label, pattern in EMPLOYMENT_TYPE_PATTERNS:
        if pattern.search(text):
            return label
    return ""


# Context-aware pay extraction from description text
PAY_EXTRACT_SUBJECT = re.compile(
    r"\$\s*\d+[\d,]*(?:\.\d+)?[kKmM]?|\d+[\d,]*\s*[kKmM]\b",
    re.IGNORECASE,
)
PAY_SKIP_CONTEXT = re.compile(
    r"\b(trillion|billion|million|assets|revenue|market|funding|capital|valuation|"
    r"401\s*\(?k\)?|403\s*\(?b\)?|retirement|benefit|benefits|matching|match|"
    r"contribution|contributions|stipend|allowance|reimbursement|budget)\b",
    re.IGNORECASE,
)


def _normalize_pay_candidate(candidate: str, context: str) -> tuple[Optional[float], Optional[str], Optional[str]]:
    raw = candidate.strip()
    if PAY_SKIP_CONTEXT.search(context):
        return None, None, None
    if re.search(r"\b(?:401|403)\s*[kKbB]\b|\b(?:401|403)\s*\([kKbB]\)", raw):
        return None, None, None

    value = raw.replace("$", "").strip().lower().replace(",", "")
    multiplier = 1
    if value.endswith("k"):
        multiplier = 1000
        value = value[:-1]
    elif value.endswith("m"):
        multiplier = 1_000_000
        value = value[:-1]
    try:
        numeric = float(value)
    except Exception:
        return None, None, None
    normalized = numeric * multiplier

    if normalized < 100:
        if not re.search(r"\b(hour|hourly|/hr|per hour)\b", context, re.IGNORECASE):
            return None, None, None
    if 0 < normalized < 20 and re.search(r"\b0?\.?\d{1,2}[kKmM]?\b", raw):
        if not re.search(r"\b(hour|hourly|/hr|per hour)\b", context, re.IGNORECASE):
            return None, None, None

    if 50_000 <= normalized < 1_000_000:
        return normalized, "year", f"${int(normalized):,} per year"
    if 0 < normalized < 1000 and re.search(r"\b(hour|hourly|/hr|per hour)\b", context, re.IGNORECASE):
        return normalized, "hour", f"${normalized:.2f} per hour"
    return None, None, None


def _extract_pay_from_description(description: str) -> Optional[str]:
    if not description:
        return None
    clean_desc = _clean(description)
    candidates: list[tuple[float, str, str, bool]] = []
    for match in PAY_EXTRACT_SUBJECT.finditer(clean_desc):
        context = clean_desc[max(0, match.start() - 60):min(len(clean_desc), match.end() + 60)]
        normalized, unit, pay = _normalize_pay_candidate(match.group(0), context)
        if pay:
            is_ote = bool(re.search(r"\bOTE\b|on\s*target\s*earnings|on\s*target\s*earning\b", context, re.IGNORECASE))
            candidates.append((normalized or 0.0, pay, unit or "", is_ote))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[3], item[0]), reverse=True)
    return candidates[0][1]


# ── Single job enrich ─────────────────────────────────────────────────────────

def _enrich_one(job: dict, timeout: float) -> dict:
    """
    Try to fill in missing Pay for a single job dict.

    Dispatch order:
      1. ATS harvester via fetch._harvest_job_data (Greenhouse/Lever/Builtin/Workday)
      2. JSON-LD schema.org JobPosting salary parsing
      3. DOM selector pay extraction
      4. Description-based contextual pay extraction
      5. Pay.extract_pay on description text

    Returns the job dict (possibly mutated with a Pay value).
    """
    url = str(job.get("Url") or job.get("url") or "").strip()
    if not url:
        return job

    # Try ATS harvesters first (returns structured pay)
    try:
        ats_data = _fetch._harvest_job_data(url)
        if isinstance(ats_data, dict):
            raw_pay_candidate = str(ats_data.get("pay") or "").strip()
            pay_candidate = _pay_extract(raw_pay_candidate) if raw_pay_candidate else ""
            if pay_candidate:
                job["Pay"] = pay_candidate
                return job
            # If description was fetched but no pay, try pay extraction on it
            desc = str(ats_data.get("description") or "").strip()
            if desc:
                inferred = _extract_pay_from_description(desc) or _pay_extract(desc)
                if inferred:
                    job["Pay"] = inferred
                    return job
    except Exception:
        pass

    # Fall back to direct HTTP fetch for JSON-LD / DOM parsing
    try:
        res = requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"},
        )
    except Exception:
        return job

    if not res.ok or "text/html" not in res.headers.get("Content-Type", "").lower():
        return job

    soup = BeautifulSoup(res.text, "html.parser")

    # JSON-LD salary (most structured)
    ld = _json_ld(soup, url)
    if ld:
        pay_candidate = str(ld.get("pay") or "").strip()
        if pay_candidate:
            job["Pay"] = pay_candidate
            return job
        # Try description-based extraction from the JSON-LD description
        desc = str(ld.get("description") or "").strip()
        if desc:
            inferred = _extract_pay_from_description(desc) or _pay_extract(desc)
            if inferred:
                job["Pay"] = inferred
                return job

    # DOM pay extraction
    pay_candidate = _extract_pay_text(soup)
    if pay_candidate:
        job["Pay"] = pay_candidate
        return job

    # Description-based contextual extraction
    desc = _extract_description(soup)
    if desc:
        inferred = _extract_pay_from_description(desc) or _pay_extract(desc)
        if inferred:
            job["Pay"] = inferred
            return job

    return job


# ── Batch enrich entrypoint ───────────────────────────────────────────────────

def enrich_apply_tier(jobs: List[dict], verbose: bool = True) -> List[dict]:
    """
    Second-pass pay enrichment for jobs where Pay is missing.

    Operates in-place on the list[dict] passed in; also returns the list.
    Call BEFORE the accept/reject split in pipeline.py.

    Args:
        jobs:    list of job record dicts (same format as CSV rows)
        verbose: print progress

    Returns:
        The same list with Pay fields backfilled where found.
    """
    candidates = [j for j in jobs if not str(j.get("Pay") or "").strip()]
    if not candidates:
        if verbose:
            print("[enrich] All jobs already have Pay — skipping enrichment pass.")
        return jobs

    workers = _HARVEST_DEFAULTS["DETAIL_ENRICH_WORKERS"]
    timeout = _HARVEST_DEFAULTS["DETAIL_ENRICH_TIMEOUT"]

    if verbose:
        print(f"[enrich] Enriching {len(candidates)} jobs missing Pay "
              f"({workers} workers, {timeout}s timeout)")

    enriched = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_enrich_one, job, timeout): job for job in candidates}
        total = len(futures)
        for i, fut in enumerate(as_completed(futures), 1):
            job = futures[fut]
            try:
                result = fut.result()
            except Exception:
                result = job
            if str(result.get("Pay") or "").strip():
                enriched += 1
                if verbose:
                    url_snippet = str(result.get("Url") or result.get("url") or "")[:80]
                    print(f"  [{i}/{total}] ok  {url_snippet}  pay={result['Pay']}")
            else:
                failed += 1
                if verbose:
                    url_snippet = str(result.get("Url") or result.get("url") or "")[:80]
                    print(f"  [{i}/{total}] no  {url_snippet}")

    if verbose:
        print(f"[enrich] Done — {enriched} enriched, {failed} no pay found")

    return jobs
