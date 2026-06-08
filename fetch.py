"""
fetch.py — HTTP fetchers, ATS harvesters, category inference, lifecycle state.

Merged from:
  pull_helpers.py
  harvest_soft/harvesters/greenhouse.py
  harvest_soft/harvesters/lever.py
  harvest_soft/harvesters/workday.py
  harvest_soft/harvesters/builtin.py
  harvest_soft/harvesters/ashby.py
  harvest_soft/harvesters/bamboohr.py
  harvest_soft/harvesters/icims.py
  harvest_soft/harvesters/jobvite.py

Public API (used by pipeline.py and enrich.py):
    _harvest_job_data(url, default_title, default_location) -> dict
    fetch_text(url) -> str
    infer_job_category(title, location, exp_lvl, pay, tier, description="") -> str
    load_previous_rows(*paths) -> dict
    load_lifecycle_state(path) -> tuple
    save_lifecycle_state(path, runs, dropped) -> None
    _is_real_url(url) -> bool
    _is_reject_pay(pay) -> bool
    _build_reject_reason(score_result, pay) -> str
    _normalize_title(title) -> str
    _is_fuzzy_dupe(candidate, seen) -> bool
    INPUT_PATTERNS
"""

import csv
import difflib
import json
import os
import random
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.exceptions import ParserRejectedMarkup

from Pay import extract_pay

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DELAY = 1.2
TIMEOUT = 15
INPUT_PATTERNS = (
    "job-results*.csv",
    "job results*.csv",
    "results*.csv",
)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_ALT_UAS = [
    _UA,
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/605.1.15",
]

_SALARY_HINTS = re.compile(
    r"\b(?:salary|compensation|pay|wage|base\s+salary|annual|year|USD|CAD|per\s*year|per\s*hr|hour)\b",
    re.IGNORECASE,
)

_SCRIPT_PAY_HINTS = re.compile(
    r"\b(?:salary|compensation|base\s*salary|base\s*pay|wage|pay\s*range|min(?:imum)?\s*salary|max(?:imum)?\s*salary|estimated\s*salary)\b",
    re.IGNORECASE,
)

_local = threading.local()
_RATE_LOCK = threading.Lock()
_last_request_time: float = 0.0

# ---------------------------------------------------------------------------
# Shared JSON-LD helpers (used across multiple harvesters)
# ---------------------------------------------------------------------------

def _jsonld_location_to_text(location_value) -> str:
    if isinstance(location_value, str):
        return location_value.strip()
    if isinstance(location_value, dict):
        addr = location_value.get("address")
        if isinstance(addr, dict):
            parts = [
                str(addr.get("streetAddress") or "").strip(),
                str(addr.get("addressLocality") or "").strip(),
                str(addr.get("addressRegion") or "").strip(),
                str(addr.get("postalCode") or "").strip(),
                str(addr.get("addressCountry") or "").strip(),
            ]
            joined = ", ".join([p for p in parts if p])
            if joined:
                return joined
        for key in ("name", "addressLocality", "addressRegion", "addressCountry"):
            raw = location_value.get(key)
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
        return ""
    if isinstance(location_value, list):
        parts = [_jsonld_location_to_text(item) for item in location_value]
        parts = [p for p in parts if p]
        return " | ".join(parts)
    return ""


def _jsonld_salary_to_text(json_ld: dict) -> str:
    if not isinstance(json_ld, dict):
        return ""
    base = json_ld.get("baseSalary") or json_ld.get("base_salary") or json_ld.get("salary")
    if not base:
        return ""

    def _as_num(v):
        try:
            if isinstance(v, (int, float)):
                return float(v)
            if isinstance(v, str):
                return float(v.replace(",", "").strip())
        except Exception:
            return None
        return None

    unit = ""
    low = None
    high = None

    if isinstance(base, dict):
        unit = str(base.get("unitText") or base.get("unit_text") or "").strip()
        val = base.get("value")
        if isinstance(val, dict):
            low = _as_num(val.get("minValue") or val.get("min_value") or val.get("value"))
            high = _as_num(val.get("maxValue") or val.get("max_value"))
            if not unit:
                unit = str(val.get("unitText") or val.get("unit_text") or "").strip()
        else:
            low = _as_num(val)
    else:
        low = _as_num(base)

    if low is None and high is None:
        return ""

    unit_l = unit.lower()
    if "hour" in unit_l:
        unit_out = "hour"
    elif "year" in unit_l or "annual" in unit_l:
        unit_out = "year"
    else:
        unit_out = "year" if (low or 0) >= 1000 else ""

    if high is not None and low is not None and high >= low:
        return f"${int(low):,}-${int(high):,} {unit_out}".strip()
    if low is not None:
        return f"${int(low):,} {unit_out}".strip()
    return ""


def normalize_pay_text(raw_pay: str) -> str:
    text = (raw_pay or "").strip()
    if not text:
        return ""
    canonical = extract_pay(text)
    if canonical:
        return canonical
    lower = text.lower()
    if lower in {
        "competitive",
        "doe",
        "tbd",
        "negotiable",
        "dependent on experience",
        "market rate",
    }:
        return text
    return ""


def _extract_age_hours(text: str) -> Optional[float]:
    if not text:
        return None
    lowered = re.sub(r"\s+", " ", text.lower())
    m = re.search(r"(\d+)\s*(minute|minutes|hour|hours|day|days|week|weeks)\s+ago", lowered)
    if not m:
        return None
    qty = int(m.group(1))
    unit = m.group(2)
    if "minute" in unit:
        return qty / 60.0
    if "hour" in unit:
        return float(qty)
    if "day" in unit:
        return float(qty * 24)
    if "week" in unit:
        return float(qty * 24 * 7)
    return None


def _is_recent_hint(text: str, max_post_age_hours: int) -> bool:
    if max_post_age_hours <= 0:
        return True
    age = _extract_age_hours(text)
    if age is None:
        return True
    return age <= max_post_age_hours


# ============================================================================
# SECTION 1 — Greenhouse harvester
# ============================================================================

class GreenhouseClient:
    BASE_API = "https://api.greenhouse.io/v1/boards"

    def __init__(self, host: Optional[str] = None, timeout: int = 5):
        self.host = host or "boards.greenhouse.io"
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})

    def _get_json(self, url: str) -> Dict[str, Any]:
        r = self.session.get(url, timeout=self.timeout)
        if r.status_code == 404:
            return {"jobs": []}
        if r.status_code != 200:
            raise requests.HTTPError(f"{r.status_code} for {url}\n{r.text[:300]}")
        if "application/json" not in r.headers.get("content-type", ""):
            raise ValueError(f"Expected JSON from {url}")
        return r.json()

    def _fetch_with_fallback(self, primary: str, fallback: Optional[str]) -> Dict[str, Any]:
        try:
            return self._get_json(primary)
        except Exception:
            if fallback:
                return self._get_json(fallback)
            raise

    def fetch_job(self, slug: str, job_id: str) -> Optional[Dict[str, Any]]:
        primary = f"{self.BASE_API}/{slug}/jobs/{job_id}"
        fallback = f"https://job-boards.greenhouse.io/{slug}/jobs/{job_id}"
        try:
            data = self._fetch_with_fallback(primary, fallback)
        except Exception:
            return None
        if not data or not isinstance(data, dict):
            return None
        title = str(data.get("title") or "").strip()
        offices = data.get("offices") or []
        location = (
            str(offices[0].get("name") or "") if isinstance(offices, list) and offices else ""
        ).strip()

        pay = ""
        for field in (data.get("metadata") or []):
            if not isinstance(field, dict):
                continue
            name_lower = str(field.get("name") or "").lower()
            if any(kw in name_lower for kw in ("salary", "pay", "compensation", "wage")):
                val = field.get("value")
                if isinstance(val, str) and val.strip():
                    pay = val.strip()
                    break

        desc_html = data.get("description") or data.get("content") or ""
        if desc_html:
            soup = BeautifulSoup(desc_html, "html.parser")
            description = soup.get_text(separator=" ", strip=True)
        else:
            description = ""

        return {
            "title": title,
            "company": slug,
            "url": f"https://boards.greenhouse.io/{slug}/jobs/{job_id}",
            "pay": pay,
            "description": description,
            "location": location,
        }


# ============================================================================
# SECTION 2 — Lever harvester
# ============================================================================

_LEVER_BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
}


def _lever_to_iso(value) -> str:
    try:
        ms = int(value)
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return ""


def _lever_remote_type(job: Dict) -> str:
    categories = job.get("categories") if isinstance(job.get("categories"), dict) else {}
    text = " ".join([
        str(categories.get("location") or ""),
        str(categories.get("commitment") or ""),
        str(job.get("workplaceType") or ""),
        str(job.get("text") or ""),
    ]).lower()
    if "remote" in text:
        return "Remote"
    if "hybrid" in text:
        return "Hybrid"
    return ""


def lever_pull(slug: str, session: Optional[requests.Session] = None, limit: Optional[int] = None) -> List[Dict]:
    active_session = session or requests.Session()
    url = f"https://api.lever.co/v0/postings/{slug}"
    response = active_session.get(url, headers=_LEVER_BASE_HEADERS, params={"mode": "json"}, timeout=25)
    if response.status_code == 404:
        return []
    if response.status_code != 200:
        response.raise_for_status()
    try:
        rows = response.json()
    except Exception:
        return []
    if not isinstance(rows, list):
        return []
    out: List[Dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        categories = row.get("categories") if isinstance(row.get("categories"), dict) else {}
        posted_value = _lever_to_iso(row.get("createdAt"))
        remote_type = _lever_remote_type(row)
        pay_text = str(categories.get("salary") or row.get("salary") or row.get("compensation") or "").strip()
        if not pay_text and isinstance(row.get("metadata"), dict):
            pay_text = str(row["metadata"].get("salary") or row["metadata"].get("compensation") or "").strip()
        out.append({
            "source": "lever",
            "slug": slug,
            "source_id": str(row.get("id") or ""),
            "title": str(row.get("text") or ""),
            "url": str(row.get("hostedUrl") or ""),
            "description": str(row.get("description") or row.get("text") or "").strip(),
            "locations_text": str(categories.get("location") or ""),
            "workplaceType": remote_type,
            "remote_type": remote_type,
            "posted_at": posted_value,
            "pay": pay_text,
            "raw_payload": row,
        })
        if isinstance(limit, int) and limit > 0 and len(out) >= limit:
            break
    return out


# ============================================================================
# SECTION 3 — Workday harvester
# ============================================================================

def workday_parse_slug(slug: str):
    parts = str(slug).split("|")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raw = (slug or "").strip()
    if not raw:
        return None
    if "://" not in raw:
        raw = f"https://{raw.lstrip('/')}"
    parsed = urlparse(raw)
    if not parsed.netloc:
        return None
    host = parsed.netloc.lower()
    if "myworkdayjobs.com" not in host:
        return None
    path_parts = [p for p in (parsed.path or "").split("/") if p]
    if not path_parts:
        return None
    locale_re = re.compile(r"^[a-z]{2}-[a-z]{2}$", flags=re.IGNORECASE)
    if locale_re.match(path_parts[0]) and len(path_parts) >= 2:
        site = path_parts[1]
    else:
        site = path_parts[0]
    tenant = host.split(".", 1)[0]
    base = f"{(parsed.scheme or 'https').lower()}://{parsed.netloc}"
    return base, tenant, site


def _wd_normalize_base(base: str) -> str:
    return base.rstrip("/")


def _wd_build_browser_headers(base: str, site: str) -> dict:
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0",
        "accept-language": "en-US",
        "origin": _wd_normalize_base(base),
        "referer": f"{_wd_normalize_base(base)}/{site}",
    }


def _wd_extract_cxs_from_html(html: str):
    m = re.search(r"/wday/cxs/([^/\"'?#]+)/([^/\"'?#]+)/jobs", html or "", flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1), m.group(2)


def _wd_resolve_board(session: requests.Session, board_url: str, base: str, tenant: str, site: str):
    cache = getattr(session, "_workday_board_cache", {})
    key = f"workday::{board_url}"
    if key in cache:
        return cache[key]
    default_board = f"{_wd_normalize_base(base)}/{site}"
    board_candidates = [default_board, f"{_wd_normalize_base(base)}/en-US/{site}"]
    last_response = None
    for candidate in board_candidates:
        try:
            last_response = session.get(candidate, headers={"user-agent": "Mozilla/5.0"}, timeout=20)
            if last_response.ok:
                break
        except Exception:
            continue
    resolved_tenant = tenant
    resolved_site = site
    referer = default_board
    if last_response is not None:
        referer = last_response.url or default_board
        cxs = _wd_extract_cxs_from_html(last_response.text)
        if cxs:
            resolved_tenant, resolved_site = cxs
        else:
            parsed = urlparse(referer)
            parts = [p for p in (parsed.path or "").split("/") if p]
            locale_re = re.compile(r"^[a-z]{2}-[a-z]{2}$", flags=re.IGNORECASE)
            if parts:
                if locale_re.match(parts[0]) and len(parts) >= 2:
                    resolved_site = parts[1]
                else:
                    resolved_site = parts[0]
    value = (resolved_tenant, resolved_site, referer)
    cache[key] = value
    setattr(session, "_workday_board_cache", cache)
    return value


def workday_fetch_job(board_url: str, external_path: str, session: requests.Session = None):
    parsed = workday_parse_slug(board_url)
    if not parsed:
        return None
    base, tenant, site = parsed
    base = _wd_normalize_base(base)
    session = session or requests.Session()
    tenant, site, referer = _wd_resolve_board(session, board_url, base, tenant, site)
    job_url = f"{base}/wday/cxs/{tenant}/{site}{external_path}"
    headers = _wd_build_browser_headers(base, site)
    headers["referer"] = referer
    res = session.get(job_url, headers=headers, timeout=20)
    if not res.ok:
        return None
    data = res.json()
    j = data.get("jobPosting") or data.get("jobPostingInfo") or data.get("jobPostingDetails") or data
    if isinstance(j, dict) and "jobPostingInfo" in j:
        j = j["jobPostingInfo"]
    post_date = j.get("postedOn") or j.get("startDate")
    location = j.get("location") or (j.get("jobRequisitionLocation") or {}).get("descriptor")
    remote_type = j.get("remoteType")
    if not remote_type and "remote" in str(location or "").lower():
        remote_type = "Remote"
    return {
        "source": "workday",
        "url": j.get("externalUrl") or "",
        "description": str(j.get("description") or j.get("jobDescription") or j.get("comments") or "").strip(),
        "workplaceType": remote_type or "",
        "remote_type": remote_type or "",
        "posted_at": post_date,
        "location": str(location or "").strip(),
        "raw_payload": j,
    }


# ============================================================================
# SECTION 4 — BuiltIn harvester
# ============================================================================

_BUILTIN_BASE = "https://builtin.com"
_BUILTIN_SESSION = requests.Session()
_BUILTIN_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
})


def _builtin_parse_salary(pay_text: Optional[str]):
    salary_min = salary_max = currency = None
    if not pay_text:
        return salary_min, salary_max, currency
    match = re.search(r"(\d+)K-(\d+)K", pay_text)
    if match:
        salary_min = int(match.group(1)) * 1000
        salary_max = int(match.group(2)) * 1000
        currency = "USD"
    return salary_min, salary_max, currency


def _builtin_parse_locations(soup: BeautifulSoup):
    locations = []
    tooltip_span = soup.select_one("span[data-bs-toggle='tooltip']")
    if tooltip_span:
        tooltip_html = tooltip_span.get("title", "")
        tooltip_soup = BeautifulSoup(tooltip_html, "html.parser")
        for div in tooltip_soup.find_all("div"):
            text = div.get_text(strip=True)
            if text:
                locations.append(text)
    if not locations:
        for script in soup.select("script[type='application/ld+json']"):
            raw_text = (script.string or script.get_text() or "").strip()
            if not raw_text:
                continue
            try:
                payload = json.loads(raw_text)
            except Exception:
                continue
            records = payload if isinstance(payload, list) else [payload]
            for record in records:
                if not isinstance(record, dict):
                    continue
                job_locations = record.get("jobLocation")
                if not isinstance(job_locations, list):
                    continue
                for item in job_locations:
                    if not isinstance(item, dict):
                        continue
                    addr = item.get("address")
                    if not isinstance(addr, dict):
                        continue
                    parts = [
                        str(addr.get("addressLocality", "") or "").strip(),
                        str(addr.get("addressRegion", "") or "").strip(),
                        str(addr.get("addressCountry", "") or "").strip(),
                    ]
                    text = ", ".join([p for p in parts if p])
                    if text:
                        locations.append(text)
    if not locations:
        candidate_remote = None
        for span in soup.find_all("span"):
            text = span.get_text(strip=True)
            lowered = text.lower()
            if not text or len(text) > 80:
                continue
            if "employee" in lowered:
                continue
            if any(term in lowered for term in ("manager", "engineer", "director", "analyst", "specialist", "intern")):
                continue
            if re.search(r",\s*[A-Z]{2}\b", text) or "united states" in lowered or "usa" in lowered:
                locations.append(text)
                break
            if lowered == "remote" or lowered.startswith("remote "):
                candidate_remote = text
        if not locations and candidate_remote:
            locations.append(candidate_remote)
    return ", ".join(locations) if locations else None


def builtin_parse_detail(url: str) -> Optional[Dict]:
    try:
        r = _BUILTIN_SESSION.get(url, timeout=(5, 10))
        r.raise_for_status()
    except Exception:
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    title = None
    title_tag = soup.select_one("h1 span")
    if title_tag:
        title = title_tag.get_text(strip=True)
    if not title:
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)
    company = None
    company_tag = soup.select_one("a[href^='/company/'] h2")
    if company_tag:
        company = company_tag.get_text(strip=True)
    if not company:
        company_anchor = soup.select_one("a[href^='/company/']")
        if company_anchor:
            company = company_anchor.get_text(strip=True)
    pay_raw = None
    for span in soup.find_all("span"):
        txt = span.get_text(strip=True)
        if "Annually" in txt or "Hourly" in txt:
            pay_raw = txt
            break
    salary_min, salary_max, currency = _builtin_parse_salary(pay_raw)
    location = _builtin_parse_locations(soup)
    employment_type = None
    for span in soup.find_all("span"):
        text = span.get_text(strip=True)
        lowered = text.lower()
        if lowered in {"remote", "hybrid", "onsite", "on-site", "in office", "in-office"}:
            employment_type = text
            break
        if "hiring remotely" in lowered:
            employment_type = "remote"
            break
    description = None
    desc_div = soup.find("div", id=lambda x: x and x.startswith("job-post-body-"))
    if desc_div:
        description = desc_div.get_text("\n", strip=True)
    if not description:
        desc_alt = soup.select_one(".html-parsed-content")
        if desc_alt:
            description = desc_alt.get_text("\n", strip=True)
    if not description:
        article = soup.find("article")
        if article:
            description = article.get_text("\n", strip=True)
    parsed_url = urlparse(url)
    external_id = parsed_url.path.rstrip("/").split("/")[-1]
    return {
        "platform": "builtin",
        "external_id": external_id,
        "url": url,
        "title": title,
        "company": company,
        "location": location,
        "employment_type": employment_type,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "currency": currency,
        "description": description,
        "raw_payload": {"pay_raw": pay_raw},
    }


# ============================================================================
# SECTION 5 — Ashby harvester
# ============================================================================

_ASHBY_URL = "https://jobs.ashbyhq.com/api/non-user-graphql"
_ASHBY_BASE_HEADERS = {
    "accept": "*/*",
    "content-type": "application/json",
    "apollographql-client-name": "frontend_non_user",
    "apollographql-client-version": "0.1.0",
    "origin": "https://jobs.ashbyhq.com",
    "user-agent": "Mozilla/5.0",
}

_ASHBY_BOARD_QUERY = """
query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
  jobBoard: jobBoardWithTeams(
    organizationHostedJobsPageName: $organizationHostedJobsPageName
  ) {
    jobPostings {
      id
      title
      locationName
      employmentType
      compensationTierSummary
    }
  }
}
"""

_ASHBY_DETAIL_QUERY = """
query ApiJobPosting($organizationHostedJobsPageName: String!, $jobPostingId: String!) {
  jobPosting(
    organizationHostedJobsPageName: $organizationHostedJobsPageName
    jobPostingId: $jobPostingId
  ) {
    id
    title
    publishedDate
    compensationTierSummary
    scrapeableCompensationSalarySummary
    descriptionHtml
  }
}
"""


def _ashby_build_referer(variables: dict) -> str:
    org = variables.get("organizationHostedJobsPageName")
    if not org:
        return "https://jobs.ashbyhq.com"
    job_id = variables.get("jobPostingId")
    if job_id:
        return f"https://jobs.ashbyhq.com/{org}/{job_id}"
    return f"https://jobs.ashbyhq.com/{org}"


def _ashby_request(operation_name: str, query: str, variables: dict) -> dict:
    headers = _ASHBY_BASE_HEADERS.copy()
    headers["referer"] = _ashby_build_referer(variables)
    max_retries = max(0, int(os.getenv("ASHBY_RETRY_429", "3")))
    base_backoff = float(os.getenv("ASHBY_RETRY_BASE_SECONDS", "1.25"))
    timeout_s = float(os.getenv("ASHBY_TIMEOUT_SECONDS", "12"))
    attempt = 0
    while True:
        try:
            res = requests.post(
                f"{_ASHBY_URL}?op={operation_name}",
                headers=headers,
                json={"operationName": operation_name, "query": query, "variables": variables},
                timeout=timeout_s,
            )
        except requests.RequestException:
            if attempt >= max_retries:
                raise
            time.sleep(base_backoff * (2 ** attempt) + random.uniform(0.0, 0.35))
            attempt += 1
            continue
        if res.status_code == 429 or 500 <= res.status_code < 600:
            if attempt >= max_retries:
                raise requests.HTTPError(f"Ashby {operation_name} failed ({res.status_code})", response=res)
            retry_after = res.headers.get("Retry-After")
            try:
                sleep_s = float(retry_after) if retry_after else base_backoff * (2 ** attempt)
            except (TypeError, ValueError):
                sleep_s = base_backoff * (2 ** attempt)
            time.sleep(sleep_s + random.uniform(0.0, 0.35))
            attempt += 1
            continue
        if not res.ok:
            raise requests.HTTPError(f"Ashby {operation_name} failed ({res.status_code})", response=res)
        payload = res.json()
        if payload.get("data"):
            return payload["data"]
        errors = payload.get("errors")
        raise RuntimeError(f"Ashby {operation_name} returned no data" + (f": {errors}" if errors else ""))


def _ashby_infer_workplace(location_name: Optional[str]) -> str:
    loc = str(location_name or "").strip().lower()
    if not loc:
        return ""
    if "remote" in loc:
        return "Remote"
    if "hybrid" in loc:
        return "Hybrid"
    if "on-site" in loc or "onsite" in loc:
        return "On-Site"
    return ""


def ashby_pull(slug: str, limit: Optional[int] = None) -> List[Dict]:
    try:
        board_data = _ashby_request("ApiJobBoardWithTeams", _ASHBY_BOARD_QUERY, {"organizationHostedJobsPageName": slug})
    except Exception:
        return []
    job_board = board_data.get("jobBoard") or {}
    postings = list(job_board.get("jobPostings", []))

    def _priority(job: Dict) -> int:
        wt = _ashby_infer_workplace(job.get("locationName"))
        return 0 if wt == "Remote" else (1 if wt == "Hybrid" else 2)

    postings.sort(key=_priority)
    if limit:
        postings = postings[:limit]

    def fetch_detail(job: Dict) -> Optional[Dict]:
        job_id = job.get("id")
        if not job_id:
            return None
        try:
            detail_data = _ashby_request("ApiJobPosting", _ASHBY_DETAIL_QUERY, {
                "organizationHostedJobsPageName": slug,
                "jobPostingId": job_id,
            })
        except Exception:
            return None
        job_posting = detail_data.get("jobPosting") or {}
        date_posted = job_posting.get("publishedDate")
        if not date_posted:
            return None
        workplace_type = _ashby_infer_workplace(job.get("locationName"))
        pay = job_posting.get("compensationTierSummary", "") or job.get("compensationTierSummary", "")
        pay = pay or job_posting.get("scrapeableCompensationSalarySummary", "")
        description = job_posting.get("descriptionHtml") or job_posting.get("description") or ""
        if isinstance(description, str):
            description = description.strip()
        return {
            "source": "ashby",
            "slug": slug,
            "source_id": job_id,
            "title": job.get("title", ""),
            "url": f"https://jobs.ashbyhq.com/{slug}/{job_id}",
            "workplaceType": workplace_type,
            "remote_type": workplace_type,
            "employment_type": job.get("employmentType", ""),
            "pay": pay,
            "description": description,
            "date_posted": date_posted,
        }

    results: List[Dict] = []
    detail_workers = max(1, int(os.getenv("ASHBY_DETAIL_MAX_WORKERS", "3")))
    with ThreadPoolExecutor(max_workers=detail_workers) as executor:
        futures = [executor.submit(fetch_detail, job) for job in postings]
        for future in as_completed(futures):
            row = future.result()
            if row:
                results.append(row)
    return results


# ============================================================================
# SECTION 6 — BambooHR harvester
# ============================================================================

_BAMBOO_BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
}
_BAMBOO_TIMEOUT = float(os.getenv("BAMBOOHR_TIMEOUT_SECONDS", "12"))
_BAMBOO_RAISE = os.getenv("BAMBOOHR_RAISE_ERRORS", "0") == "1"
_BAMBOO_FETCH_DETAIL = os.getenv("BAMBOOHR_FETCH_DETAIL", "1") == "1"
_BAMBOO_DETAIL_MAX = int(os.getenv("BAMBOOHR_DETAIL_MAX_JOBS", "5"))
_BAMBOO_JSON_LD_RE = re.compile(
    r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)


def _bamboo_coalesce(*values) -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _bamboo_location_text(raw_location) -> str:
    if isinstance(raw_location, dict):
        parts = [p for p in (
            str(raw_location.get("city") or "").strip(),
            str(raw_location.get("state") or "").strip(),
            str(raw_location.get("country") or "").strip(),
        ) if p]
        return ", ".join(parts)
    if isinstance(raw_location, str):
        return raw_location.strip()
    return ""


def _bamboo_to_iso_date(value) -> str:
    if isinstance(value, str) and value.strip():
        v = value.strip()
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            return v
    return ""


def _bamboo_remote_type(location_text: str, title: str) -> str:
    text = f"{location_text} {title}".lower()
    if "remote" in text:
        return "Remote"
    if "hybrid" in text:
        return "Hybrid"
    return ""


def _bamboo_extract_json_ld(html: str) -> Optional[dict]:
    if not html:
        return None
    m = _BAMBOO_JSON_LD_RE.search(html)
    if not m:
        return None
    raw = (m.group(1) or "").strip()
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except Exception:
        return None
    if isinstance(data, dict) and data.get("@type") == "JobPosting":
        return data
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and item.get("@type") == "JobPosting":
                return item
    return None


def _bamboo_fallback_salary(html: str) -> str:
    if not html:
        return ""
    m = re.search(
        r"(?:\$\s*[\d,]+(?:[kK])?(?:\s*[\-\u2013to]+\s*\$\s*[\d,]+(?:[kK])?)?|(?:USD|EUR|GBP)\s+[\d,]+)"
        r"(?:\s*(?:per\s+)?(?:year|yr|annual|hour|hr|month|mo|weekly|wk|daily|day))?",
        html, re.IGNORECASE,
    )
    if m:
        return m.group(0).strip()
    return ""


def _bamboo_fetch_detail(slug: str, job_id: str, session: requests.Session) -> Dict[str, str]:
    json_url = f"https://{slug}.bamboohr.com/careers/{job_id}/detail"
    try:
        r = session.get(json_url, headers=_BAMBOO_BASE_HEADERS, timeout=_BAMBOO_TIMEOUT)
        if r.status_code == 200 and "application/json" in r.headers.get("Content-Type", ""):
            data = r.json()
            result = data.get("result") or {}
            job_obj = result.get("jobOpening") or result.get("job") or result
            if isinstance(job_obj, dict):
                title = str(job_obj.get("jobOpeningName") or job_obj.get("title") or "").strip()
                pay_text = str(job_obj.get("compensation") or "").strip()
                dept = job_obj.get("location") or {}
                if isinstance(dept, dict):
                    loc_parts = [str(dept.get("city") or ""), str(dept.get("state") or "")]
                    location_text = ", ".join(p for p in loc_parts if p)
                else:
                    location_text = str(dept or "")
                return {
                    "detail_title": title,
                    "detail_location": location_text,
                    "detail_description": str(job_obj.get("description") or "").strip(),
                    "detail_pay": pay_text,
                }
    except Exception:
        pass
    html_url = f"https://{slug}.bamboohr.com/jobs/view/{job_id}"
    try:
        r = session.get(html_url, headers={"User-Agent": _BAMBOO_BASE_HEADERS["User-Agent"], "Accept": "text/html,*/*"}, timeout=_BAMBOO_TIMEOUT)
        r.raise_for_status()
        html = r.text or ""
        json_ld = _bamboo_extract_json_ld(html) or {}
        title = str(json_ld.get("title") or json_ld.get("name") or "").strip()
        location_text = _jsonld_location_to_text(json_ld.get("jobLocation"))
        pay_text = _jsonld_salary_to_text(json_ld)
        if not pay_text:
            pay_text = _bamboo_fallback_salary(html)
        return {
            "detail_title": title,
            "detail_location": location_text,
            "detail_description": str(json_ld.get("description") or "").strip(),
            "detail_pay": pay_text,
        }
    except Exception:
        return {"detail_title": "", "detail_location": "", "detail_description": "", "detail_pay": ""}


def bamboohr_pull(slug: str, session: Optional[requests.Session] = None, limit: Optional[int] = None) -> List[Dict]:
    active_session = session or requests.Session()
    url = f"https://{slug}.bamboohr.com/careers/list"
    try:
        response = active_session.get(url, headers=_BAMBOO_BASE_HEADERS, timeout=_BAMBOO_TIMEOUT)
        if response.status_code != 200:
            return []
        if "application/json" not in response.headers.get("Content-Type", ""):
            return []
        data = response.json()
    except Exception:
        if _BAMBOO_RAISE:
            raise
        return []
    rows = data.get("result", [])
    if not isinstance(rows, list):
        return []
    out: List[Dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        job_id = row.get("id")
        title = _bamboo_coalesce(row.get("jobOpeningName"), row.get("title"))
        location_text = _bamboo_location_text(row.get("location"))
        remote_type = _bamboo_remote_type(location_text, title)
        posted_value = _bamboo_to_iso_date(_bamboo_coalesce(row.get("postingDate"), row.get("datePosted"), row.get("createdDate")))
        job_url = f"https://{slug}.bamboohr.com/jobs/view/{job_id}" if job_id else f"https://{slug}.bamboohr.com/careers"
        pay_value = _bamboo_coalesce(row.get("pay"), row.get("salary"), row.get("compensation"), row.get("salary_min"), row.get("salary_max"))
        out.append({
            "source": "bamboohr",
            "slug": slug,
            "source_id": str(job_id or ""),
            "title": title,
            "url": job_url,
            "description": row.get("description") or "",
            "pay": pay_value,
            "locations_text": location_text,
            "workplaceType": remote_type,
            "remote_type": remote_type,
            "posted_at": posted_value,
            "date_posted": posted_value,
        })
        if isinstance(limit, int) and limit > 0 and len(out) >= limit:
            break
    if _BAMBOO_FETCH_DETAIL and out:
        max_jobs = max(0, min(_BAMBOO_DETAIL_MAX, len(out)))
        for job in out[:max_jobs]:
            jid = str(job.get("source_id") or "").strip()
            if not jid:
                continue
            try:
                detail = _bamboo_fetch_detail(slug, jid, active_session)
            except Exception:
                if _BAMBOO_RAISE:
                    raise
                continue
            if detail.get("detail_title") and not job.get("title"):
                job["title"] = detail["detail_title"]
            if detail.get("detail_location") and not job.get("locations_text"):
                job["locations_text"] = detail["detail_location"]
            if detail.get("detail_description") and not job.get("description"):
                job["description"] = detail["detail_description"]
            if detail.get("detail_pay") and not job.get("pay"):
                job["pay"] = detail["detail_pay"]
            job["remote_type"] = _bamboo_remote_type(str(job.get("locations_text") or ""), str(job.get("title") or ""))
            job["workplaceType"] = job["remote_type"]
    return out


# ============================================================================
# SECTION 7 — iCIMS harvester
# ============================================================================

_ICIMS_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "text/html"}
_ICIMS_JSON_LD_RE = re.compile(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', re.S)
_ICIMS_JOB_LINK_RE = re.compile(r'href=["\']([^"\']*/jobs/(\d+)[^"\']*/job(?:\?[^"\']*)?)["\']', re.I)
_ICIMS_TIMEOUT = float(os.getenv("ICIMS_TIMEOUT_SECONDS", "8"))
_ICIMS_DELAY_MIN = float(os.getenv("ICIMS_DELAY_MIN_SECONDS", "0.35"))
_ICIMS_DELAY_MAX = float(os.getenv("ICIMS_DELAY_MAX_SECONDS", "0.90"))
_ICIMS_MAX_RETRIES = int(os.getenv("ICIMS_MAX_RETRIES", "2"))
_ICIMS_BACKOFF_BASE = float(os.getenv("ICIMS_BACKOFF_BASE_SECONDS", "1.5"))
_ICIMS_MAX_ROWS = int(os.getenv("ICIMS_MAX_DISCOVERED_ROWS_PER_SLUG", "120"))
_ICIMS_MAX_DETAIL = int(os.getenv("ICIMS_MAX_DETAIL_ATTEMPTS_PER_SLUG", "30"))
_ICIMS_RETRYABLE = {429, 502, 503, 504}


def _icims_strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = re.sub(r"&nbsp;|&#160;", " ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def _icims_extract_header_fields(html: str) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    pattern = re.compile(
        r'<div[^>]*class="[^"]*iCIMS_JobHeaderTag[^"]*"[^>]*>.*?'
        r'<dt[^>]*>(.*?)</dt>.*?<dd[^>]*>(.*?)</dd>.*?</div>',
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(html or ""):
        label = _icims_strip_html(match.group(1)).lower().strip(": ")
        value = _icims_strip_html(match.group(2))
        if label and value:
            fields[label] = value
    return fields


def _icims_pace() -> None:
    low = max(0.0, _ICIMS_DELAY_MIN)
    high = max(low, _ICIMS_DELAY_MAX)
    if high > 0:
        time.sleep(random.uniform(low, high))


def _icims_safe_get(session: requests.Session, url: str) -> Optional[requests.Response]:
    for attempt in range(_ICIMS_MAX_RETRIES + 1):
        _icims_pace()
        try:
            response = session.get(url, headers=_ICIMS_HEADERS, timeout=_ICIMS_TIMEOUT)
        except requests.RequestException:
            if attempt >= _ICIMS_MAX_RETRIES:
                return None
            time.sleep(_ICIMS_BACKOFF_BASE * (2 ** attempt))
            continue
        if response.status_code in _ICIMS_RETRYABLE and attempt < _ICIMS_MAX_RETRIES:
            delay = _ICIMS_BACKOFF_BASE * (2 ** attempt)
            time.sleep(delay)
            continue
        return response
    return None


def _icims_discover_jobs(slug: str, session: Optional[requests.Session] = None) -> List[Dict]:
    base = f"https://careers-{slug}.icims.com"
    search_url = f"{base}/jobs/search"
    active_session = session or requests.Session()
    r = _icims_safe_get(active_session, search_url)
    if not r or r.status_code != 200:
        return []
    html = r.text
    jobs: Dict[str, Dict[str, str]] = {}
    for match in _ICIMS_JOB_LINK_RE.finditer(html):
        job_id = match.group(2)
        start = max(0, match.start() - 220)
        end = min(len(html), match.end() + 220)
        hint_text = re.sub(r"<[^>]+>", " ", html[start:end])
        jobs[job_id] = {"job_id": job_id, "hint_text": re.sub(r"\s+", " ", hint_text).strip()}
    if not jobs:
        iframe_match = re.search(r'<iframe[^>]+src="([^"]*jobs/search\?in_iframe=1[^"]*)"', html, re.I)
        if iframe_match:
            iframe_url = iframe_match.group(1)
            if iframe_url.startswith("/"):
                iframe_url = base + iframe_url
            iframe_res = _icims_safe_get(active_session, iframe_url)
            if iframe_res and iframe_res.status_code == 200:
                iframe_html = iframe_res.text
                for match in _ICIMS_JOB_LINK_RE.finditer(iframe_html):
                    job_id = match.group(2)
                    start = max(0, match.start() - 220)
                    end = min(len(iframe_html), match.end() + 220)
                    hint_text = re.sub(r"<[^>]+>", " ", iframe_html[start:end])
                    jobs[job_id] = {"job_id": job_id, "hint_text": re.sub(r"\s+", " ", hint_text).strip()}
    return list(jobs.values())


def _icims_fetch_job(slug: str, job_id: str, session: Optional[requests.Session] = None) -> Optional[Dict]:
    url = f"https://careers-{slug}.icims.com/jobs/{job_id}/job?in_iframe=1"
    active_session = session or requests.Session()
    r = _icims_safe_get(active_session, url)
    if not r or r.status_code != 200:
        return None
    html = r.text
    match = _ICIMS_JSON_LD_RE.search(html)
    if not match:
        return None
    try:
        data = json.loads(match.group(1))
    except Exception:
        return None
    if data.get("@type") != "JobPosting":
        return None
    title = (data.get("title") or data.get("name") or "").strip()
    header_fields = _icims_extract_header_fields(html)
    remote_status = (header_fields.get("remote status") or header_fields.get("remote") or header_fields.get("workplace type") or "").strip()
    location_bits = []
    for key in ("location", "location : address", "location : city", "location : state", "location : postal code", "location : country"):
        val = (header_fields.get(key) or "").strip()
        if val:
            location_bits.append(val)
    location_text = " ".join(location_bits).strip()
    if not location_text:
        location_text = _jsonld_location_to_text(data.get("jobLocation"))
    if not remote_status and location_text:
        lowered_loc = location_text.lower()
        if "remote" in lowered_loc:
            remote_status = "Remote"
        elif "hybrid" in lowered_loc:
            remote_status = "Hybrid"
        elif "on-site" in lowered_loc or "onsite" in lowered_loc:
            remote_status = "On-Site"
    pay_text = _jsonld_salary_to_text(data)
    return {
        "source": "icims",
        "slug": slug,
        "source_id": job_id,
        "title": title,
        "url": url,
        "location": location_text,
        "description": str(data.get("description") or "").strip(),
        "pay": pay_text,
        "workplaceType": remote_status,
        "remote_type": remote_status,
        "posted_at": data.get("datePosted"),
        "date_posted": data.get("datePosted"),
    }


def icims_pull(slug: str, max_post_age_hours: int = 0, limit: Optional[int] = None) -> List[Dict]:
    with requests.Session() as session:
        job_rows = _icims_discover_jobs(slug, session=session)
        if _ICIMS_MAX_ROWS > 0 and len(job_rows) > _ICIMS_MAX_ROWS:
            job_rows = job_rows[:_ICIMS_MAX_ROWS]
        results: List[Dict] = []
        detail_attempts = 0
        for row in job_rows:
            if limit is not None and limit > 0 and len(results) >= limit:
                break
            if _ICIMS_MAX_DETAIL > 0 and detail_attempts >= _ICIMS_MAX_DETAIL:
                break
            job_id = row.get("job_id")
            if not job_id:
                continue
            hint_text = row.get("hint_text") or ""
            if max_post_age_hours > 0 and not _is_recent_hint(hint_text, max_post_age_hours=max_post_age_hours):
                continue
            detail_attempts += 1
            job = _icims_fetch_job(slug, job_id, session=session)
            if job:
                results.append(job)
    return results[:limit] if isinstance(limit, int) and limit > 0 else results


# ============================================================================
# SECTION 8 — Jobvite harvester
# ============================================================================

_JOBVITE_BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/json",
}


def _jobvite_parse_json_ld(soup: BeautifulSoup) -> Optional[dict]:
    script = soup.find("script", type="application/ld+json")
    if not script:
        return None
    try:
        data = json.loads(script.string)
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                return None
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("@type") == "JobPosting":
                    return item
        elif isinstance(data, dict) and data.get("@type") == "JobPosting":
            return data
    except Exception:
        return None
    return None


def _jobvite_extract_posted_date(json_ld: dict) -> Optional[str]:
    if not json_ld:
        return None
    date = json_ld.get("datePosted")
    if not date:
        return None
    try:
        return datetime.fromisoformat(date.replace("Z", "+00:00")).isoformat()
    except Exception:
        return date


def _jobvite_fetch_listing(slug: str, session: requests.Session) -> List[Dict]:
    listing_url = f"https://jobs.jobvite.com/{slug}"
    r = session.get(listing_url, headers=_JOBVITE_BASE_HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    seen = set()
    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not re.search(rf"/{slug}/job/\w+", href):
            continue
        full_url = urljoin(listing_url, href)
        if full_url in seen:
            continue
        seen.add(full_url)
        parent = a.parent
        parent_text = parent.get_text(" ", strip=True) if parent else ""
        candidates.append({"url": full_url, "hint_text": parent_text})
    return candidates


def _jobvite_fetch_detail(url: str, slug: str, session: requests.Session) -> Dict:
    r = session.get(url, headers=_JOBVITE_BASE_HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    json_ld = _jobvite_parse_json_ld(soup)
    date_posted = _jobvite_extract_posted_date(json_ld)
    title = ""
    location_text = ""
    pay_text = ""
    if isinstance(json_ld, dict):
        title = (json_ld.get("title") or json_ld.get("name") or "").strip()
        location_text = _jsonld_location_to_text(json_ld.get("jobLocation"))
        pay_text = _jsonld_salary_to_text(json_ld)
    job_id = None
    if json_ld:
        identifier = json_ld.get("identifier")
        if isinstance(identifier, dict):
            job_id = identifier.get("value")
        elif isinstance(identifier, str):
            job_id = identifier
    return {
        "source": "jobvite",
        "slug": slug,
        "source_id": job_id,
        "title": title,
        "url": url,
        "location": location_text,
        "description": str(json_ld.get("description") or "").strip() if isinstance(json_ld, dict) else "",
        "pay": pay_text,
        "posted_at": date_posted,
        "date_posted": date_posted,
    }


def jobvite_pull(slug: str, session: Optional[requests.Session] = None, max_post_age_hours: int = 0) -> List[Dict]:
    session = session or requests.Session()
    jobs: List[Dict] = []
    candidates = _jobvite_fetch_listing(slug, session)
    for candidate in candidates:
        url = candidate.get("url")
        hint_text = candidate.get("hint_text") or ""
        if max_post_age_hours > 0 and not _is_recent_hint(hint_text, max_post_age_hours=max_post_age_hours):
            continue
        try:
            jobs.append(_jobvite_fetch_detail(url, slug, session))
        except Exception:
            continue
    return jobs


# ============================================================================
# SECTION 9 — HTTP utilities (from pull_helpers.py)
# ============================================================================

def _session() -> requests.Session:
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update({"User-Agent": _UA})
        _local.session = s
    return _local.session


def _throttle() -> None:
    global _last_request_time
    with _RATE_LOCK:
        now = time.monotonic()
        wait = DELAY - (now - _last_request_time)
        if wait > 0:
            time.sleep(wait)
        _last_request_time = time.monotonic()


def _browser_headers(url: str, ua: Optional[str] = None) -> dict:
    parsed = urllib.parse.urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    return {
        "User-Agent": ua or _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": origin,
        "Origin": origin,
        "Connection": "keep-alive",
    }


def _is_real_url(url: str) -> bool:
    parsed = urllib.parse.urlparse((url or "").strip())
    return bool(parsed.scheme in ("http", "https") and parsed.netloc)


def _fetch_with_alternate_ua(url: str) -> str:
    for ua in _ALT_UAS[1:]:
        try:
            _throttle()
            r = _session().get(url, headers=_browser_headers(url, ua), timeout=TIMEOUT)
            html = _response_html_text(r)
            if r.status_code == 200 and html:
                return html
        except Exception:
            continue
    return ""


def _fetch_without_browser_headers(url: str) -> str:
    try:
        _throttle()
        r = requests.get(url, timeout=TIMEOUT)
        html = _response_html_text(r)
        if r.status_code == 200 and html:
            return html
    except Exception:
        pass
    return ""


def _response_html_text(response: requests.Response) -> str:
    content_type = (response.headers.get("Content-Type") or "").lower()
    text = response.text or ""
    if len(text) <= 500:
        return ""
    head = text[:256].lstrip()
    if not head:
        return ""
    if re.search(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", head):
        return ""
    looks_like_html = head.startswith("<") or "<html" in head.lower() or "<!doctype html" in head.lower()
    if "html" not in content_type and "xml" not in content_type and not looks_like_html:
        return ""
    return text


def _fetch_html(url: str) -> str:
    try:
        _throttle()
        r = _session().get(url, headers=_browser_headers(url), timeout=TIMEOUT)
        html = _response_html_text(r)
        if r.status_code == 200 and html:
            return html
    except Exception:
        pass
    return _fetch_with_alternate_ua(url)


def _normalize_space(text: str) -> str:
    return " ".join(str(text).split())


def _extract_fallback_text(soup) -> str:
    chunks = []
    for tag in soup.find_all(["p", "li", "span", "div"], limit=200):
        t = tag.get_text(separator=" ", strip=True)
        if len(t) > 40:
            chunks.append(t)
    return " ".join(chunks[:80])


def _contains_salary_clue(text: str) -> bool:
    return bool(_SALARY_HINTS.search(text))


def _extract_structured_salary_text(soup) -> str:
    snippets: list[str] = []
    for script in soup.find_all("script"):
        script_type = (script.get("type") or "").lower()
        raw = script.string or script.get_text(separator=" ", strip=False) or ""
        if not raw:
            continue
        lowered = raw.lower()
        if "ld+json" not in script_type and not _SCRIPT_PAY_HINTS.search(lowered):
            continue
        if len(raw) > 200000:
            continue
        compact = re.sub(r"\s+", " ", raw)
        if not _SCRIPT_PAY_HINTS.search(compact):
            continue
        for match in _SCRIPT_PAY_HINTS.finditer(compact):
            start = max(0, match.start() - 140)
            end = min(len(compact), match.end() + 220)
            snippets.append(compact[start:end])
            if len(snippets) >= 40:
                break
        if len(snippets) >= 40:
            break
    return " ".join(snippets[:40])


def _extract_meta_salary_text(soup) -> str:
    bits: list[str] = []
    for meta in soup.find_all("meta"):
        content = (meta.get("content") or "").strip()
        if content and _contains_salary_clue(content):
            bits.append(content)
    return " ".join(bits[:20])


def _extract_pay_from_html(url: str) -> str:
    html = _fetch_html(url)
    if not html:
        return ""
    pay = extract_pay(html)
    if pay:
        return pay
    try:
        soup = BeautifulSoup(html, "html.parser")
    except ParserRejectedMarkup:
        return ""
    visible_text = _normalize_space(soup.get_text(separator=" ", strip=True))
    if visible_text:
        pay = extract_pay(visible_text)
        if pay:
            return pay
    return ""


def fetch_text(url: str) -> str:
    """Download url and return cleaned plain text, or '' on error."""
    for attempt in range(2):
        try:
            _throttle()
            r = _session().get(url, headers=_browser_headers(url), timeout=TIMEOUT)
            if r.status_code != 200:
                fallback = _fetch_with_alternate_ua(url)
                if not fallback:
                    return ""
                html = fallback
            else:
                html = r.text
                if "\ufffd" in html:
                    fallback = _fetch_without_browser_headers(url)
                    if fallback:
                        html = fallback
            soup = BeautifulSoup(html, "html.parser")
            structured_salary = _extract_structured_salary_text(soup)
            meta_salary = _extract_meta_salary_text(soup)
            for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
                tag.decompose()
            visible = soup.get_text(separator=" ", strip=True)
            if structured_salary:
                visible = f"{visible} {structured_salary}".strip()
            if meta_salary:
                visible = f"{visible} {meta_salary}".strip()
            if len(visible) < 200 or not _contains_salary_clue(visible):
                visible = " ".join([visible, _extract_fallback_text(soup)]).strip()
            if not _contains_salary_clue(visible) and _contains_salary_clue(html):
                visible = " ".join([visible, html]).strip()
            return visible
        except Exception:
            continue
    return ""


# ============================================================================
# SECTION 10 — ATS routing (from pull_helpers.py)
# ============================================================================

_GH_BOARD_URL_RE = re.compile(
    r"(?:job-boards\.greenhouse\.io|boards\.greenhouse\.io)/([^/?#&]+)/jobs/(\d+)",
    re.IGNORECASE,
)
_GH_JID_RE = re.compile(r"[?&]gh_jid=(\d+)", re.IGNORECASE)
_LEVER_URL_RE = re.compile(
    r"(?:jobs\.lever\.co|lever\.co)/([^/?#&]+)/([0-9a-f-]{30,})",
    re.IGNORECASE,
)


def _detect_greenhouse(url: str) -> Optional[Tuple[str, str]]:
    m = _GH_BOARD_URL_RE.search(url)
    if m:
        return m.group(1).lower(), m.group(2)
    m_jid = _GH_JID_RE.search(url)
    if m_jid:
        job_id = m_jid.group(1)
        parsed = urllib.parse.urlparse(url)
        host = parsed.netloc.lower().lstrip("www.")
        slug = host.split(".")[0]
        return slug, job_id
    return None


def _detect_lever(url: str) -> Optional[Tuple[str, str]]:
    m = _LEVER_URL_RE.search(url)
    if m:
        return m.group(1).lower(), m.group(2).lower()
    return None


def _detect_workday(url: str) -> bool:
    return "myworkdayjobs.com" in (urllib.parse.urlparse(url).netloc or "")


def _build_pay_string(min_value: Optional[int], max_value: Optional[int], currency: Optional[str]) -> str:
    if min_value is None and max_value is None:
        return ""
    symbol = "$"
    if currency and currency.upper() != "USD":
        symbol = currency.upper() + " "
    if min_value is not None and max_value is not None:
        return f"{symbol}{min_value:,.0f} \u2013 {symbol}{max_value:,.0f}/yr"
    if min_value is not None:
        return f"{symbol}{min_value:,.0f}/yr"
    if max_value is not None:
        return f"{symbol}{max_value:,.0f}/yr"
    return ""


def _harvest_greenhouse_job(url: str) -> Optional[dict]:
    detected = _detect_greenhouse(url)
    if not detected:
        return None
    slug, job_id = detected
    try:
        _throttle()
        client = GreenhouseClient()
        data = client.fetch_job(slug, job_id)
        if not data:
            return None
        return {
            "title": str(data.get("title") or "").strip(),
            "company": str(data.get("company") or slug).strip(),
            "url": url,
            "pay": str(data.get("pay") or "").strip(),
            "description": str(data.get("description") or "").strip(),
            "location": str(data.get("location") or "").strip(),
        }
    except Exception:
        return None


def _harvest_lever_job(url: str) -> Optional[dict]:
    detected = _detect_lever(url)
    if not detected:
        return None
    slug, job_id = detected
    try:
        _throttle()
        rows = lever_pull(slug, limit=200)
        if not isinstance(rows, list):
            return None
        job = next(
            (row for row in rows if isinstance(row, dict) and str(row.get("source_id") or "").lower() == job_id),
            None,
        )
        if not job:
            return None
        return {
            "title": str(job.get("title") or "").strip(),
            "company": str(job.get("slug") or slug).strip(),
            "url": url,
            "pay": str(job.get("pay") or "").strip(),
            "description": str(job.get("description") or "").strip(),
            "location": str(job.get("locations_text") or "").strip(),
        }
    except Exception:
        return None


def _harvest_builtin_job(url: str) -> Optional[dict]:
    if "builtin.com/job/" not in url:
        return None
    try:
        job = builtin_parse_detail(url)
        if not job:
            return None
        title = str(job.get("title") or "").strip()
        company = str(job.get("company") or "").strip()
        description = str(job.get("description") or "").strip()
        pay_raw = _build_pay_string(job.get("salary_min"), job.get("salary_max"), job.get("currency"))
        if not pay_raw:
            pay_raw = extract_pay(description)
        return {
            "title": title,
            "company": company,
            "url": url,
            "pay": pay_raw or "",
            "description": description,
            "location": str(job.get("location") or "").strip(),
        }
    except Exception:
        return None


def _harvest_workday_job(url: str) -> Optional[dict]:
    if not _detect_workday(url):
        return None
    parsed = urllib.parse.urlparse(url)
    path_parts = [p for p in parsed.path.split("/") if p]
    if not path_parts:
        return None
    locale_re = re.compile(r"^[a-z]{2}-[a-z]{2}$", re.IGNORECASE)
    if locale_re.match(path_parts[0]) and len(path_parts) > 1:
        path_parts = path_parts[1:]
    if len(path_parts) < 2:
        return None
    if workday_parse_slug(url) is None:
        return None
    external_path = "/" + "/".join(path_parts[1:])
    try:
        job = workday_fetch_job(url, external_path)
        if not job:
            return None
        description = str(job.get("description") or "").strip()
        pay_raw = extract_pay(description)
        return {
            "title": str(job.get("title") or "").strip(),
            "company": "",
            "url": url,
            "pay": pay_raw or "",
            "description": description,
            "location": str(job.get("location") or "").strip(),
        }
    except Exception:
        return None


def _harvest_job_data(url: str, default_title: str = "", default_location: str = "") -> dict:
    """Try ATS-specific harvesters first; fall back to generic HTML fetch."""
    job = _harvest_greenhouse_job(url)
    if not job:
        job = _harvest_lever_job(url)
    if not job:
        job = _harvest_builtin_job(url)
    if not job:
        job = _harvest_workday_job(url)
    if job:
        if not job.get("title"):
            job["title"] = default_title
        if not job.get("location"):
            job["location"] = default_location
        if not job.get("description"):
            job["description"] = fetch_text(url)
        if not job.get("pay"):
            description = job.get("description", "") or ""
            job["pay"] = extract_pay(description) or _extract_pay_from_html(url)
        job["pay"] = normalize_pay_text(job.get("pay") or "")
        return job
    text = fetch_text(url)
    pay_text = extract_pay(text)
    if not pay_text:
        pay_text = _extract_pay_from_html(url)
    pay_text = normalize_pay_text(pay_text)
    return {
        "title": default_title,
        "company": "",
        "url": url,
        "pay": pay_text or "",
        "description": text,
        "location": default_location,
    }


# ============================================================================
# SECTION 11 — Category inference (from pull_helpers.py)
# ============================================================================

CATEGORY_TITLE_PATTERNS = [
    ("Code & Build", [
        r"\bsoftware\s+engineer\b", r"\bplatform\s+engineer\b", r"\bfull[- ]stack\b",
        r"\bfrontend\b", r"\bfront[- ]end\b", r"\bbackend\b", r"\bback[- ]end\b",
        r"\bdevops\b", r"\bsite\s+reliability\b", r"\bsre\b",
        r"\b(ios|android|mobile)\s+(engineer|developer)\b",
        r"\bqa\s+(engineer|automation)\b", r"\bsoftware\s+developer\b", r"\bprogrammer\b",
    ]),
    ("Data & Insights", [
        r"\bdata\s+(analyst|engineer|scientist|architect)\b",
        r"\bbusiness\s+(analyst|intelligence)\b", r"\breporting\s+analyst\b",
        r"\banalytics\s+engineer\b", r"\bmachine\s+learning\s+engineer\b", r"\bml\s+engineer\b",
        r"\bdata\s+science\b", r"\boperations?\s+analyst\b", r"\bcompliance\s+analyst\b",
        r"\brisk\s+analyst\b", r"\bprocess\s+analyst\b", r"\bquality\s+analyst\b",
        r"\bfinancial\s+analyst\b", r"\bBI\s+(analyst|developer|engineer)\b",
    ]),
    ("IT & Systems", [
        r"\bsystems?\s+analyst\b", r"\bbusiness\s+systems?\s+analyst\b",
        r"\bIT\s+(analyst|specialist|coordinator|support)\b", r"\bsystems?\s+administrator\b",
        r"\bnetwork\s+(engineer|administrator|analyst)\b", r"\bsecurity\s+(analyst|engineer|architect)\b",
        r"\bcloud\s+(engineer|architect|administrator)\b", r"\bhelp\s*desk\b", r"\bdesktop\s+support\b",
        r"\bdatabase\s+administrator\b", r"\bEDI\b", r"\bapplication\s+support\b",
        r"\bservicenow\b", r"\bsalesforce\s+administrator\b",
    ]),
    ("Finance & Accounting", [
        r"\baccountant\b", r"\bpayroll\b", r"\baccounts\s+(payable|receivable)\b",
        r"\bfinancial\s+(analyst|manager|controller|planner)\b", r"\bcontroller\b",
        r"\bbookkeeper\b", r"\btax\s+(analyst|manager|accountant)\b", r"\bFP&A\b",
        r"\baudit\b", r"\bcontracts?\s+(specialist|negotiator|administrator)\b",
    ]),
    ("People & HR", [
        r"\bhr\s+(generalist|coordinator|specialist|manager|analyst)\b", r"\bhuman\s+resources\b",
        r"\bpeople\s+(ops|operations|partner)\b", r"\btalent\s+(acquisition|partner|ops)\b",
        r"\brecruiter\b", r"\bhrbp\b",
        r"\bbenefits\s+(analyst|coordinator|specialist|administrator)\b",
        r"\bcompensation\s+(analyst|specialist)\b",
    ]),
    ("Client Success", [
        r"\bcustomer\s+success\b", r"\bclient\s+success\b", r"\baccount\s+(executive|manager)\b",
        r"\bcustomer\s+experience\b", r"\brelationship\s+manager\b", r"\bengagement\s+manager\b",
    ]),
    ("Ops & Support", [
        r"\boperations?\s+(coordinator|specialist|manager|associate|analyst)\b",
        r"\bimplementation\s+(specialist|analyst|coordinator)\b",
        r"\bproject\s+manager\b", r"\bprogram\s+(manager|coordinator)\b",
        r"\bsupply\s+chain\b", r"\bprocurement\b", r"\blogistics\b",
        r"\bcompliance\s+(officer|specialist|coordinator)\b", r"\bworkflow\s+analyst\b",
        r"\bwfm\b", r"\bworkforce\s+management\b", r"\bscheduler\b",
        r"\bcare\s+(coordinator|navigator|manager)\b",
    ]),
]

CATEGORY_DESCRIPTION_PATTERNS = [
    ("Code & Build", [
        r"\bsoftware\s+engineer(ing)?\b", r"\bfull[- ]stack\b", r"\bfrontend\b", r"\bbackend\b",
        r"\bdevops\b", r"\bsite\s+reliability\b", r"\bsoftware\s+development\b",
    ]),
    ("Data & Insights", [
        r"\bdata\s+(analysis|analytics|reporting|warehouse|pipeline|governance|quality)\b",
        r"\bbusiness\s+intelligence\b", r"\bmachine\s+learning\b", r"\bdata\s+science\b",
        r"\bBI\s+(tool|report|dashboard)\b",
    ]),
    ("IT & Systems", [
        r"\bIT\s+(infrastructure|systems|support|security)\b",
        r"\bcloud\s+(infrastructure|platform|migration)\b", r"\bsystems?\s+administration\b",
        r"\bdatabase\s+administration\b", r"\bactive\s+directory\b",
        r"\bEDI\s+(transaction|mapping|integration)\b", r"\bservicenow\b",
    ]),
    ("Finance & Accounting", [
        r"\baccounts\s+(payable|receivable)\b",
        r"\bpayroll\s+(processing|administration|systems?)\b", r"\bgeneral\s+ledger\b",
        r"\bmonth[- ]end\s+close\b",
        r"\bfinancial\s+(reporting|statements?|modeling|planning)\b",
    ]),
    ("People & HR", [
        r"\bhuman\s+resources\b", r"\bpeople\s+operations\b",
        r"\bbenefits\s+(administration|enrollment|compliance)\b",
        r"\btalent\s+(acquisition|management|development)\b",
        r"\bcompensation\s+(and|&)\s+benefits\b",
    ]),
    ("Client Success", [
        r"\bcustomer\s+success\b", r"\bclient\s+success\b", r"\bcustomer\s+experience\b",
    ]),
    ("Ops & Support", [
        r"\boperations?\s+(process|workflow|efficiency|improvement)\b",
        r"\bproject\s+management\b", r"\bprogram\s+management\b",
        r"\bsupply\s+chain\b", r"\bprocurement\b", r"\blogistics\b",
        r"\bimplementation\s+(process|project|lifecycle)\b",
        r"\bworkflow\s+(automation|optimization|management)\b",
        r"\bcare\s+(coordinator|navigator|manager)\b",
    ]),
]

CATEGORY_TITLE_FALLBACK = [
    ("Code & Build",         [r"\bdeveloper\b", r"\bengineer\b", r"\bsoftware\b", r"\bprogrammer\b", r"\bdevops\b"]),
    ("Data & Insights",      [r"\banalytics\b", r"\banalyst\b", r"\bdata\b", r"\bscientist\b"]),
    ("IT & Systems",         [r"\bsystems?\b", r"\bnetwork\b", r"\bsecurity\b", r"\bcloud\b", r"\bEDI\b", r"\bservicenow\b", r"\bsalesforce\b"]),
    ("Finance & Accounting", [r"\baccountant\b", r"\bpayroll\b", r"\bfinancial\b", r"\baudit\b", r"\btax\b", r"\bnegotiator\b"]),
    ("People & HR",          [r"\bhuman\s+resources\b", r"\bhr\b", r"\brecruiter\b", r"\btalent\b", r"\bbenefits\b"]),
    ("Client Success",       [r"\bcustomer\s+success\b", r"\baccount\s+(executive|manager)\b"]),
    ("Ops & Support",        [r"\boperations\b", r"\bsupport\b", r"\bcoordinator\b", r"\bproject\b", r"\bprogram\b", r"\bimplementation\b", r"\bscheduler\b", r"\bwfm\b"]),
]


def infer_job_category(title: str, location: str, exp_lvl: str, pay: str, tier: str, description: str = "") -> str:
    title_text = (title or "").strip().lower()
    description_text = (description or "").strip().lower()
    if not title_text and not description_text:
        return "Other"
    for category, patterns in CATEGORY_TITLE_PATTERNS:
        for pattern in patterns:
            if re.search(pattern, title_text, re.IGNORECASE):
                return category
    if description_text:
        for category, patterns in CATEGORY_DESCRIPTION_PATTERNS:
            for pattern in patterns:
                if re.search(pattern, description_text, re.IGNORECASE):
                    return category
    for category, patterns in CATEGORY_TITLE_FALLBACK:
        for pattern in patterns:
            if re.search(pattern, title_text, re.IGNORECASE):
                return category
    return "Other"


# ============================================================================
# SECTION 12 — Pay rejection helpers (from pull_helpers.py)
# ============================================================================

def _parse_hourly_pay(pay: str) -> tuple[float | None, float | None]:
    match_hr = re.search(
        r"^\$?([\d,]+(?:\.\d+)?)(?:\s*[–—-]\s*\$?([\d,]+(?:\.\d+)?))?\s*(?:/?hr\b|per\s+hour\b)",
        pay,
        re.IGNORECASE,
    )
    if not match_hr:
        return None, None
    try:
        low = float(match_hr.group(1).replace(",", ""))
    except ValueError:
        return None, None
    high = None
    if match_hr.group(2):
        try:
            high = float(match_hr.group(2).replace(",", ""))
        except ValueError:
            high = None
    return low, high


def _is_suspicious_high_hourly_pay(pay: str) -> bool:
    max_hourly = float(os.getenv("HIGH_PAY_HARD_SKIP_HOURLY", "150"))
    suspicious_exact_values = {
        value.strip()
        for value in os.getenv("SUSPICIOUS_EXACT_HOURLY_PAY", "40").split(",")
        if value.strip()
    }
    low, high = _parse_hourly_pay(pay)
    if low is None:
        return False
    if high is None and f"{low:g}" in suspicious_exact_values:
        return True
    return low > max_hourly or (high is not None and high > max_hourly)

def _is_reject_pay(pay: str) -> bool:
    if not pay or not pay.strip():
        return True
    if re.search(r"\$0(?:[\d,]*)(?:\.\d+)?", pay):
        return True
    lo, _ = _parse_hourly_pay(pay)
    if lo is not None:
        if _is_suspicious_high_hourly_pay(pay):
            return True
        return lo < 25.0
    match_yr = re.search(
        r"^\$?([\d,]+(?:\.\d+)?)(?:\s*[–—-]\s*\$?[\d,]+(?:\.\d+)?)?\s*(?:/?yr\b|/?year\b|per\s+year\b)",
        pay,
        re.IGNORECASE,
    )
    if match_yr:
        try:
            lo = float(match_yr.group(1).replace(",", ""))
        except ValueError:
            return False
        return lo < 50000.0
    return False


def _build_reject_reason(score_result: dict, pay: str) -> str:
    if score_result.get("hard_kill"):
        return str(score_result["hard_kill"])
    if _is_reject_pay(pay):
        if not pay.strip():
            return "missing pay"
        if _is_suspicious_high_hourly_pay(pay):
            return "suspicious_hourly_pay"
        return "low pay"
    return ""


# ============================================================================
# SECTION 13 — Lifecycle state (from pull_helpers.py)
# ============================================================================

def load_previous_rows(*paths: Path) -> dict:
    rows: dict = {}
    for path in paths:
        if not path.exists():
            continue
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = (row.get("Url") or "").strip()
                if url:
                    rows[url] = row
    return rows


def load_lifecycle_state(path: Path) -> Tuple[dict, set]:
    if not path.exists():
        return {}, set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}, set()
    raw_runs = payload.get("runs") if isinstance(payload, dict) else {}
    raw_dropped = payload.get("dropped") if isinstance(payload, dict) else []
    runs: dict = {}
    if isinstance(raw_runs, dict):
        for url, count in raw_runs.items():
            if isinstance(url, str) and isinstance(count, int) and count >= 0:
                runs[url] = count
    dropped: set = set()
    if isinstance(raw_dropped, list):
        for url in raw_dropped:
            if isinstance(url, str) and url:
                dropped.add(url)
    return runs, dropped


def save_lifecycle_state(path: Path, runs: dict, dropped: set) -> None:
    payload = {"runs": dict(sorted(runs.items())), "dropped": sorted(dropped)}
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# ============================================================================
# SECTION 14 — Title dedup helpers (from pull_helpers.py)
# ============================================================================

def _normalize_title(title: str) -> str:
    t = title.lower()
    t = re.sub(
        r"\b(senior|sr|junior|jr|lead|staff|associate|principal|interim"
        r"|i|ii|iii|iv|v|1|2|3|4|5|mid|entry|level|remote)\b",
        " ", t, flags=re.IGNORECASE,
    )
    t = re.sub(r"[^a-z0-9 ]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _is_fuzzy_dupe(candidate: str, seen: List[str]) -> bool:
    if not candidate:
        return False
    for existing in seen:
        ratio = difflib.SequenceMatcher(None, candidate, existing).ratio()
        if ratio >= 0.90:
            return True
    return False
