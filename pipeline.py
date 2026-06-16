"""
pipeline.py — Full Soft Life pipeline: scrape → score → enrich → publish.

Merged from:
  pull.py              (scoring worker, run_pipeline, resolve helpers)
  run_softlife_pipeline.py  (orchestration, Supabase, JSON, SQL generation)

Entry point: python pipeline.py [flags]

Exports for smoketest.py:
    score_job, parse_date_label, normalize_exp_level, is_live_url,
    build_job_records, infer_job_category
"""

import argparse
import csv
import json
import re
import shutil
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import urlparse

import requests

sys.path.insert(0, str(Path(__file__).parent))

import fetch as H
import enrich as _enrich
from score import evaluate_job as _score_job

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent
WORKSPACE_DIR = BASE_DIR.parent

AGGREGATOR_DIR = WORKSPACE_DIR / "laughing-octo-fiesta-main"
AGGREGATOR_REPO_URL = "https://github.com/yvonnesilva404-debug/laughing-octo-fiesta.git"
AGGREGATOR_SCRIPTS_DIR = AGGREGATOR_DIR / "scripts"
AGGREGATOR_SCRAPER_SCRIPT = AGGREGATOR_SCRIPTS_DIR / "scraper.py"
AGGREGATOR_MERGE_SCRIPT = AGGREGATOR_SCRIPTS_DIR / "merge_data.py"
AGGREGATOR_EXPORT_SCRIPT = AGGREGATOR_SCRIPTS_DIR / "export_filtered.py"
AGGREGATOR_TRACKER_SCRIPT = AGGREGATOR_SCRIPTS_DIR / "track_foreign_drops.py"
AGGREGATOR_DATA_DIR = AGGREGATOR_DIR / "data"

SITE_DIR = WORKSPACE_DIR / "Soft_Creed"
SUPABASE_DIR = SITE_DIR / "supabase"

DEFAULT_CSV_IN = BASE_DIR / "job-results.csv"
DEFAULT_CSV_OUT = BASE_DIR / "softlife.csv"
DEFAULT_CSV_REJECT = BASE_DIR / "reject.csv"
DEFAULT_LIFECYCLE_STATE = BASE_DIR / "job_lifecycle_state.json"
DEFAULT_SITE_JSON = SITE_DIR / "jobs.json"
DEFAULT_SCHEMA_SQL = SUPABASE_DIR / "schema.sql"
DEFAULT_SEED_SQL = SUPABASE_DIR / "jobs_seed.sql"
DEFAULT_SUPABASE_TABLE = "jobs"

HEADERS = ["Tier", "Date", "Pay", "Title", "Exp Lvl", "Location", "Category", "Url", "New", "Reject Reason", "Part Time", "Night Time", "Non US"]
DEFAULT_WORKERS = 8

TIER_META = {
    "Soft Life": {"rank": 1, "icon": "assets/1.png"},
    "Tolerable": {"rank": 2, "icon": "assets/2.png"},
    "Warning":   {"rank": 3, "icon": "assets/3.png"},
    "Merciless": {"rank": 4, "icon": "assets/4.png"},
}

EXP_LEVEL_LABELS = {
    "intern": "Intern",
    "entry":  "Entry",
    "mid":    "Mid",
    "senior": "Senior",
}

TIER_MAP = {
    "APPLY":  "Soft Life",
    "MAYBE":  "Tolerable",
    "REVIEW": "Warning",
    "SKIP":   "Merciless",
}


# ---------------------------------------------------------------------------
# Public helpers exported for smoketest.py
# ---------------------------------------------------------------------------

# Re-export from fetch so smoketest and other callers get the 7-category version
infer_job_category = H.infer_job_category


def score_job(title: str, description: str) -> dict:
    """Thin wrapper used by smoketest.py and external callers."""
    return _score_job({
        "title":       title,
        "company":     "",
        "url":         "",
        "pay":         "",
        "description": description,
    })


def is_live_url(value: str) -> bool:
    parsed = urlparse((value or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def parse_date_label(value: str) -> tuple:
    text = (value or "").strip()
    if not text:
        return "", ""
    formats = ("%m/%d/%Y", "%Y-%m-%d", "%m/%d")
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%m/%d":
                parsed = parsed.replace(year=date.today().year)
            return f"{parsed.month}/{parsed.day}/{parsed.year}", parsed.date().isoformat()
        except ValueError:
            continue
    return text, ""


def normalize_exp_level(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    lower = text.lower()
    if lower in EXP_LEVEL_LABELS:
        return EXP_LEVEL_LABELS[lower]
    parts = [part for part in text.replace("_", " ").replace("-", " ").split() if part]
    return " ".join(part.capitalize() for part in parts)


def _is_softlife_location_eligible(location: str) -> bool:
    text = str(location or "").strip().lower()
    if not text:
        return False
    remote_markers = ("remote", "work from home", "wfh", "home-based", "home based", "anywhere")
    if not any(marker in text for marker in remote_markers):
        return False

    # Reject hybrid/onsite mix even if "remote" appears.
    if re.search(r"\b(remote|work\s+from\s+home|wfh)\b.*\b(hybrid|on[-\s]?site|in[-\s]?office|office)\b", text):
        return False

    # Allow state-level remote scope (e.g., "Remote in CA", "Remote - Texas").
    state_abbr = r"(?:al|ak|az|ar|ca|co|ct|de|fl|ga|hi|id|il|in|ia|ks|ky|la|me|md|ma|mi|mn|ms|mo|mt|ne|nv|nh|nj|nm|ny|nc|nd|oh|ok|or|pa|ri|sc|sd|tn|tx|ut|vt|va|wa|wv|wi|wy|dc)"
    state_name = (
        r"(?:alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|florida|georgia|hawaii|idaho|"
        r"illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|"
        r"missouri|montana|nebraska|nevada|new\s+hampshire|new\s+jersey|new\s+mexico|new\s+york|north\s+carolina|"
        r"north\s+dakota|ohio|oklahoma|oregon|pennsylvania|rhode\s+island|south\s+carolina|south\s+dakota|tennessee|"
        r"texas|utah|vermont|virginia|washington|west\s+virginia|wisconsin|wyoming|district\s+of\s+columbia)"
    )
    country_scope = r"(?:us|u\.s\.?|usa|united\s+states|canada)"
    scope_only = re.compile(
        rf"^(?:.*\b)?(?:remote|work\s+from\s+home|wfh|home[-\s]?based)(?:\s*(?:in|for|across|throughout|within|[-:,]))?\s*(?:{state_abbr}|{state_name}|{country_scope})\b.*$",
        re.IGNORECASE,
    )
    if scope_only.search(text):
        return True

    # Reject hyper-local remote targeting: city+state or ZIP in the location string.
    if re.search(rf"\b[a-z][a-z .'-]{{2,}},\s*{state_abbr}\b", text):
        return False
    if re.search(r"\b\d{5}(?:-\d{4})?\b", text):
        return False

    # Reject explicit "hiring remotely in ..." unless only state/country follows.
    remote_in = re.search(r"\bhiring\s+remotely\s+in\s+(.+)$", text)
    if remote_in:
        tail = remote_in.group(1).strip(" .;,")
        if not re.fullmatch(rf"(?:{state_abbr}|{state_name}|{country_scope})", tail, flags=re.IGNORECASE):
            return False

    return True


FIELD_REMOTE_TITLE_RE = re.compile(
    r"\b(?:"
    r"mechanic|technician|mobile\s+vehicle\s+condition\s+inspector|vehicle\s+condition\s+inspector|"
    r"railcar\s+repair|field\s+engineer|field\s+service|substation|equipment\s+engineer|"
    r"field\s+execution|travel\s+rn|travel\s+nurse|route\s+sales|territory\s+sales|maintenance|installer|"
    r"repair\s+technician|service\s+technician"
    r")\b",
    re.IGNORECASE,
)

FIELD_REMOTE_LOCATION_RE = re.compile(
    r"\b(?:field\s*/\s*remote|field[-\s]+remote|virtual\s+field|remote[-\s]+field)\b",
    re.IGNORECASE,
)

FIELD_REMOTE_LOCATION_CODE_RE = re.compile(
    r"\b(?:usa|us)_[a-z]{2}_remote\b",
    re.IGNORECASE,
)

FIELD_REMOTE_MARKET_SALES_RE = re.compile(
    r"\b(?:regional|territory|market)\b.{0,40}\bsales\b|\bsales\b.{0,40}\b(?:regional|territory|market)\b",
    re.IGNORECASE,
)


def _is_field_remote_listing(title: str, location: str) -> bool:
    """Reject jobs where "remote" means road/field/site territory work."""
    title_text = str(title or "").strip()
    location_text = str(location or "").strip()
    combined = f"{title_text} {location_text}"

    if FIELD_REMOTE_LOCATION_RE.search(location_text):
        return True
    if FIELD_REMOTE_LOCATION_CODE_RE.search(location_text) and FIELD_REMOTE_TITLE_RE.search(title_text):
        return True
    if FIELD_REMOTE_TITLE_RE.search(title_text) and re.search(r"\b(remote|virtual|field)\b", location_text, re.IGNORECASE):
        return True
    if re.search(r"\b(remote|virtual)\b", location_text, re.IGNORECASE) and re.search(
        r"\b(?:travel|territory|regional)\b", title_text, re.IGNORECASE
    ):
        return True
    if re.search(r"\b(?:hybrid|on[-\s]?site|onsite|in[-\s]?office)\b", combined, re.IGNORECASE):
        return True
    if FIELD_REMOTE_MARKET_SALES_RE.search(title_text):
        return True

    return False


_PART_TIME_RE = re.compile(
    r"\b(part.time|parttime|reduced.hours|half.time|pt\b|p/t\b)\b",
    re.IGNORECASE,
)
_NIGHT_TIME_RE = re.compile(
    r"\b(night\b|night.shift|evening.shift|2nd.shift|second.shift|3rd.shift|third.shift|"
    r"graveyard.shift|overnight|nightly)\b",
    re.IGNORECASE,
)

_NON_US_HINTS = {
    "canada",
    "portugal", "belgium", "japan", "china", "hong kong", "taiwan", "south korea",
    "czech republic", "czechia", "denmark", "estonia", "norway", "finland", "switzerland",
    "poland", "greece", "europe", "emea", "apac", "latam", "uk", "united kingdom",
    "ireland", "germany", "france", "spain", "italy", "netherlands", "sweden",
    "india", "australia", "new zealand", "singapore", "mexico", "brazil", "argentina",
    "philippines", "vietnam",
    # countries / regions commonly missing
    "south america", "central america", "latin america",
    "england", "scotland", "wales", "northern ireland",
    "serbia", "croatia", "slovakia", "slovenia", "bosnia", "montenegro", "albania",
    "bulgaria", "romania", "hungary", "moldova",
    "turkey", "ukraine", "belarus", "iceland", "luxembourg", "malta", "cyprus",
    "russia",
    "colombia", "peru", "chile", "uruguay", "paraguay", "bolivia", "ecuador", "venezuela",
    "costa rica", "panama", "guatemala", "honduras", "el salvador", "nicaragua", "cuba",
    "dominican republic",
    "indonesia", "thailand", "malaysia", "myanmar", "cambodia", "laos",
    "bangladesh", "pakistan", "sri lanka", "nepal", "mongolia",
    "saudi arabia", "uae", "dubai", "qatar", "kuwait", "oman", "bahrain",
    "jordan", "lebanon", "israel",
    "south africa", "nigeria", "kenya", "egypt", "morocco", "tunisia", "algeria",
    "ethiopia", "ghana", "senegal", "tanzania", "uganda", "angola", "mozambique",
    "cameroon", "madagascar",
    # common remote-X patterns
    "remote uk", "remote-europe", "remote canada", "remote germany", "remote france",
    "remote india", "remote australia", "remote ireland", "remote netherlands",
    "remote sweden", "remote spain", "remote italy", "remote portugal", "remote belgium",
    "remote switzerland", "remote austria", "remote poland", "remote brazil", "remote mexico",
    "remote singapore", "remote japan",
}
_NON_US_HINTS_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(h) for h in sorted(_NON_US_HINTS, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)
_NON_US_COUNTRY_CODES = {
    # non-conflicting ISO country codes (not in US state abbreviations)
    "uk", "gb", "fr", "es", "it", "nl", "se", "no", "dk", "fi", "pl",
    "au", "nz", "sg", "mx", "br", "jp", "cn", "kr", "ie", "ch",
    "at", "be", "pt", "gr", "cz", "ae", "za", "ng", "ke",
    "can",  # Canada (safe — not a US state code)
    "ru", "th", "vn", "my", "ph", "hk", "tw",
    "ee", "lt", "lv", "sk", "hr", "hu", "ro", "bg", "rs", "ua", "tr",
    "cl", "pe", "ec", "cr", "do",
    "eg", "ma", "tn", "dz", "ke", "ng", "za",
    "sa", "qa", "kw", "om", "bh", "jo", "lb", "ps",
}


def _is_part_time_job(title: str, employment_type: str = "", description: str = "") -> bool:
    combined = f"{title} {employment_type} {description}"
    return bool(_PART_TIME_RE.search(combined))


def _is_night_time_job(title: str, description: str = "") -> bool:
    combined = f"{title} {description}"
    return bool(_NIGHT_TIME_RE.search(combined))


_US_INDICATORS = {
    "usa", "united states", "u.s.", "us",
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga",
    "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
    "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj",
    "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc",
    "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy",
    "dc",
}


def _is_non_us_location(location: str) -> bool:
    text = str(location or "").strip().lower()
    if not text:
        return False
    tokens = re.sub(r"[.,\-;()]", " ", text).split()
    if any(t in _US_INDICATORS for t in tokens):
        return False
    if _NON_US_HINTS_RE.search(text):
        return True
    if any(token in _NON_US_COUNTRY_CODES for token in tokens):
        return True
    return False


# ---------------------------------------------------------------------------
# Resolve helpers
# ---------------------------------------------------------------------------

def resolve_input_csv(path_value: Optional[str] = None) -> Path:
    if path_value:
        candidate = Path(path_value)
        if not candidate.is_absolute():
            candidate = BASE_DIR / candidate
        return candidate.resolve()
    if DEFAULT_CSV_IN.exists():
        return DEFAULT_CSV_IN
    candidates: list = []
    for pattern in H.INPUT_PATTERNS:
        candidates.extend(BASE_DIR.glob(pattern))
    if not candidates:
        return DEFAULT_CSV_IN
    return max(candidates, key=lambda p: p.stat().st_mtime)


def resolve_output_path(path_value: Optional[str], default_path: Path) -> Path:
    if not path_value:
        return default_path
    candidate = Path(path_value)
    if not candidate.is_absolute():
        candidate = BASE_DIR / candidate
    return candidate.resolve()


def _resolve_path(path_value: str, default_base: Path) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = default_base / path
    return path.resolve()


# ---------------------------------------------------------------------------
# Scoring pipeline
# ---------------------------------------------------------------------------

def _run_pipeline(
    csv_in: Path,
    csv_out: Path,
    csv_reject: Path,
    workers: int = DEFAULT_WORKERS,
    use_cache: bool = True,
) -> dict:
    """
    Core scoring pass:
      1. Load source CSV
      2. Dedup + lifecycle filtering
      3. Parallel fetch + score (unique jobs only)
      4. enrich.enrich_apply_tier  ← fills missing Pay BEFORE accept/reject split
            5. Accept / reject split on policy + pay
      6. Write softlife.csv + reject.csv
    """
    previous_rows = H.load_previous_rows(csv_out, csv_reject) if use_cache else {}
    previous_reject_urls = set(H.load_previous_rows(csv_reject).keys()) if use_cache else set()
    lifecycle_runs, lifecycle_dropped = H.load_lifecycle_state(DEFAULT_LIFECYCLE_STATE)

    with open(csv_in, newline="", encoding="utf-8") as f:
        rows_in = list(csv.DictReader(f))

    total = len(rows_in)
    print(f"Processing {total} jobs with {workers} workers from {csv_in.name}…\n")

    parsed: list = []
    for row in rows_in:
        parsed.append({
            "title":    (row.get("Title")            or "").strip(),
            "location": (row.get("Location")         or "").strip(),
            "exp_lvl":  (row.get("Experience Level") or "").strip(),
            "date_val": (row.get("Date")             or "").strip(),
            "url":      (row.get("URL")              or "").strip(),
        })

    unique_jobs: list = []
    unique_index: dict = {}
    seen_norm_titles: list = []
    row_sources: list = []

    for p in parsed:
        url = p["url"]
        norm_title = H._normalize_title(p["title"])

        if url and url in lifecycle_dropped:
            row_sources.append(("dropped", url))
        elif use_cache and url and url in previous_rows:
            row_sources.append(("cached", url))
        elif url and url in unique_index:
            row_sources.append(("duplicate", unique_index[url]))
        elif H._is_fuzzy_dupe(norm_title, seen_norm_titles):
            row_sources.append(("duplicate", len(unique_jobs) - 1))
        else:
            if url:
                unique_index[url] = len(unique_jobs)
            seen_norm_titles.append(norm_title)
            unique_jobs.append(p)
            row_sources.append(("unique", len(unique_jobs) - 1))

    _print_lock = threading.Lock()
    completed = [0]

    def process(idx: int, p: dict) -> tuple:
        if H._is_real_url(p["url"]):
            job_data = H._harvest_job_data(
                p["url"],
                default_title=p["title"],
                default_location=p["location"],
            )
        else:
            job_data = {
                "title":           p["title"],
                "company":         "",
                "url":             p["url"],
                "pay":             "",
                "description":     "",
                "location":        p["location"],
                "employment_type": "",
            }
        score_result = _score_job(job_data)
        tier = TIER_MAP.get(score_result.get("tier", ""), score_result.get("tier", "Merciless"))
        pay = job_data.get("pay", "") or ""
        description = job_data.get("description", "") or ""
        employment_type = str(job_data.get("employment_type") or "")
        category = H.infer_job_category(
            p["title"], p["location"], p["exp_lvl"], pay, tier, description
        )
        reject_reason = H._build_reject_reason(score_result, pay)
        part_time = "TRUE" if _is_part_time_job(p["title"], employment_type, description) else ""
        night_time = "TRUE" if _is_night_time_job(p["title"], description) else ""
        non_us = "TRUE" if _is_non_us_location(p["location"]) else ""
        return (idx, {
            "Tier":          tier,
            "Date":          p["date_val"],
            "Pay":           pay,
            "Title":         p["title"],
            "Exp Lvl":       p["exp_lvl"],
            "Location":      p["location"],
            "Category":      category,
            "Url":           p["url"],
            "New":           "New",
            "Reject Reason": reject_reason,
            "Part Time":     part_time,
            "Night Time":    night_time,
            "Non US":        non_us,
        })

    unique_results: list = [None] * len(unique_jobs)
    if unique_jobs:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(process, idx, p): idx for idx, p in enumerate(unique_jobs)}
            for fut in as_completed(futures):
                idx, record = fut.result()
                unique_results[idx] = record

    url_run_counts: dict = {}
    for p in parsed:
        url = p["url"]
        if not url or url in lifecycle_dropped or url in url_run_counts:
            continue
        url_run_counts[url] = lifecycle_runs.get(url, 0) + 1

    slot_results: list = [None] * total

    for idx, p in enumerate(parsed):
        source_type, source_key = row_sources[idx]
        url = p["url"]

        if source_type == "dropped":
            continue

        run_count = url_run_counts.get(url, 1) if url else 1
        if url and run_count >= 4:
            lifecycle_dropped.add(url)
            lifecycle_runs[url] = run_count
            continue

        if url:
            lifecycle_runs[url] = run_count

        if source_type == "cached":
            record = previous_rows[source_key].copy()
            record["Date"]     = p["date_val"]
            record["Title"]    = p["title"]
            record["Exp Lvl"]  = p["exp_lvl"]
            record["Location"] = p["location"]
            record["Pay"]      = H.normalize_pay_text(record.get("Pay", ""))
            record["Category"] = H.infer_job_category(
                p["title"], p["location"], p["exp_lvl"],
                record.get("Pay", ""), record.get("Tier", ""),
            )
            record["Url"] = p["url"]
            record["New"] = "New" if run_count == 1 else ""
            record["Part Time"]  = record.get("Part Time", "") or ("TRUE" if _is_part_time_job(p["title"]) else "")
            record["Night Time"] = record.get("Night Time", "") or ("TRUE" if _is_night_time_job(p["title"]) else "")
            record["Non US"]     = record.get("Non US", "") or ("TRUE" if _is_non_us_location(p["location"]) else "")
            slot_results[idx] = record
        elif source_type == "duplicate":
            pass
        else:
            record = unique_results[source_key]
            if record is None:
                continue
            record = record.copy()
            record["New"] = "New" if run_count == 1 else ""
            slot_results[idx] = record

        if slot_results[idx] is not None:
            rec = slot_results[idx]
            with _print_lock:
                completed[0] += 1
                new_marker = " NEW" if rec.get("New") else ""
                print(f"[{completed[0]:>3}/{total}] {rec['Tier']:<10} {rec['Title'][:55]}{new_marker}")

    results = [r for r in slot_results if r is not None]

    # ── Enrichment pass: fill missing Pay BEFORE accept/reject split ──────────
    results = _enrich.enrich_apply_tier(results, verbose=True)

    # ── Accept / reject split ─────────────────────────────────────────────────
    accepted = []
    rejected = []
    for r in results:
        if _is_field_remote_listing(r.get("Title", ""), r.get("Location", "")):
            r["Tier"] = "Merciless"
            r["Reject Reason"] = "field_remote_not_true_remote"
            rejected.append(r)
            continue
        if r.get("Tier") == "Soft Life" and not _is_softlife_location_eligible(r.get("Location", "")):
            r["Tier"] = "Merciless"
            r["Reject Reason"] = "softlife_requires_true_remote"
            if r.get("Non US") != "TRUE":
                rejected.append(r)
                continue
        if H._is_reject_pay(r.get("Pay", "")):
            rejected.append(r)
        else:
            accepted.append(r)

    with open(csv_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(accepted)

    with open(csv_reject, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rejected)

    H.save_lifecycle_state(DEFAULT_LIFECYCLE_STATE, lifecycle_runs, lifecycle_dropped)

    tiers: dict = {}
    for r in accepted:
        tiers[r["Tier"]] = tiers.get(r["Tier"], 0) + 1

    print(f"\nDone — {len(accepted)} jobs written to {csv_out.name}")
    print(f"Rejected {len(rejected)} jobs to {csv_reject.name}")
    for tier_name in ("Soft Life", "Tolerable", "Warning", "Merciless"):
        count = tiers.get(tier_name, 0)
        if count:
            print(f"  {tier_name}: {count}")

    return {
        "input_csv":      csv_in,
        "output_csv":     csv_out,
        "reject_csv":     csv_reject,
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "tiers":          tiers,
    }


# ---------------------------------------------------------------------------
# Aggregator helpers
# ---------------------------------------------------------------------------

def pull_aggregator_data() -> None:
    if AGGREGATOR_DIR.exists():
        subprocess.run(["git", "-C", str(AGGREGATOR_DIR), "pull", "origin", "main"], check=True)
    else:
        subprocess.run(["git", "clone", AGGREGATOR_REPO_URL, str(AGGREGATOR_DIR)], check=True)


def export_source_csv(output_path: Path, freshness_days: int) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run([
        sys.executable,
        str(AGGREGATOR_EXPORT_SCRIPT),
        "--data-dir", str(AGGREGATOR_DATA_DIR),
        "--output",   str(output_path),
        "--freshness-days", str(freshness_days),
        "--include-non-us",
    ], check=True, cwd=str(AGGREGATOR_SCRIPTS_DIR))
    return output_path


def run_scraper(scrape_source: str, scrape_platform: str, scrape_limit: int) -> None:
    command = [sys.executable, str(AGGREGATOR_SCRAPER_SCRIPT), "--source", scrape_source, "--platform", scrape_platform]
    if scrape_limit > 0:
        command.extend(["--limit", str(scrape_limit)])
    subprocess.run(command, check=True, cwd=str(AGGREGATOR_DIR))


def merge_scraped_jobs() -> None:
    subprocess.run([sys.executable, str(AGGREGATOR_MERGE_SCRIPT)], check=True, cwd=str(AGGREGATOR_DIR))


def run_drop_tracker() -> None:
    subprocess.run([sys.executable, str(AGGREGATOR_TRACKER_SCRIPT)], check=True, cwd=str(AGGREGATOR_DIR))


# ---------------------------------------------------------------------------
# Site JSON + SQL generation
# ---------------------------------------------------------------------------

def build_job_records(csv_path: Path) -> list:
    rows: list = []
    seen_urls: set = set()
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            tier = (row.get("Tier") or "").strip() or "Merciless"
            tier_meta = TIER_META.get(tier, TIER_META["Merciless"])
            date_label, posted_date = parse_date_label(row.get("Date") or "")
            url = (row.get("Url") or row.get("URL") or "").strip()
            if url and url in seen_urls:
                continue
            if url:
                seen_urls.add(url)
            exp_level = normalize_exp_level(row.get("Exp Lvl") or row.get("Experience Level") or "")
            is_new = ((row.get("New") or "").strip().lower() == "new")
            category = (row.get("Category") or "").strip()
            if not category:
                category = H.infer_job_category(
                    (row.get("Title") or "").strip(),
                    (row.get("Location") or "").strip(),
                    exp_level,
                    (row.get("Pay") or "").strip(),
                    tier,
                )
            part_time = (row.get("Part Time") or "").strip().upper() == "TRUE"
            night_time = (row.get("Night Time") or "").strip().upper() == "TRUE"
            non_us = (row.get("Non US") or "").strip().upper() == "TRUE"
            rows.append({
                "date":        date_label,
                "date_label":  date_label,
                "posted_date": posted_date,
                "tier":        tier,
                "tier_rank":   tier_meta["rank"],
                "tier_icon":   tier_meta["icon"],
                "pay":         (row.get("Pay") or "").strip() or "TBD",
                "title":       (row.get("Title") or "").strip(),
                "exp_level":   exp_level,
                "location":    (row.get("Location") or "").strip(),
                "category":    category,
                "url":         url,
                "has_live_url": is_live_url(url),
                "is_new":      is_new,
                "part_time":   part_time,
                "night_time":  night_time,
                "non_us":      non_us,
            })

    def sort_key(r: dict) -> tuple:
        ordinal = 0
        if r["posted_date"]:
            try:
                ordinal = date.fromisoformat(str(r["posted_date"])).toordinal()
            except Exception:
                pass
        return (-ordinal, int(r["tier_rank"]), str(r["title"]).lower())

    rows.sort(key=sort_key)
    return rows


def write_jobs_json(rows: list, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")


def _sql_literal(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def write_seed_sql(rows: list, output_path: Path, table_name: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "-- Generated by pipeline.py",
        "begin;",
        f"delete from public.{table_name};",
    ]
    if rows:
        lines.append(
            f"insert into public.{table_name} "
            "(date_label, posted_date, tier, tier_rank, tier_icon, pay, title, "
            "exp_level, location, category, url, has_live_url, is_new, "
            "part_time, night_time, non_us)"
        )
        lines.append("values")
        value_lines = []
        for row in rows:
            value_lines.append(
                "  ("
                + ", ".join([
                    _sql_literal(row["date"]),
                    _sql_literal(row["posted_date"] or None),
                    _sql_literal(row["tier"]),
                    _sql_literal(row["tier_rank"]),
                    _sql_literal(row["tier_icon"]),
                    _sql_literal(row["pay"]),
                    _sql_literal(row["title"]),
                    _sql_literal(row["exp_level"]),
                    _sql_literal(row["location"]),
                    _sql_literal(row.get("category") or "Other"),
                    _sql_literal(row["url"]),
                    _sql_literal(row["has_live_url"]),
                    _sql_literal(row["is_new"]),
                    _sql_literal(row["part_time"]),
                    _sql_literal(row["night_time"]),
                    _sql_literal(row["non_us"]),
                ])
                + ")"
            )
        lines.append(",\n".join(value_lines) + ";")
    lines.append("commit;")
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _chunked(rows: Iterable, size: int) -> Iterable:
    bucket: list = []
    for row in rows:
        bucket.append(row)
        if len(bucket) >= size:
            yield bucket
            bucket = []
    if bucket:
        yield bucket


def upload_to_supabase(rows: list, url: str, service_key: str, table_name: str) -> None:
    base_endpoint = f"{url.rstrip('/')}/rest/v1/{table_name}"
    delete_headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Prefer": "return=minimal",
    }
    requests.delete(f"{base_endpoint}?id=not.is.null", headers=delete_headers, timeout=30).raise_for_status()

    endpoint = f"{base_endpoint}?on_conflict=url"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    for batch in _chunked(rows, 250):
        payload = [
            {
                "date_label":   row["date_label"],
                "posted_date":  row["posted_date"] or None,
                "tier":         row["tier"],
                "tier_rank":    row["tier_rank"],
                "tier_icon":    row["tier_icon"],
                "pay":          row["pay"],
                "title":        row["title"],
                "exp_level":    row["exp_level"],
                "location":     row["location"],
                "category":     row.get("category") or "Other",
                "url":          row["url"],
                "has_live_url": row["has_live_url"],
                "is_new":       row["is_new"],
                "part_time":    row["part_time"],
                "night_time":   row["night_time"],
                "non_us":       row["non_us"],
            }
            for row in batch
        ]
        requests.post(endpoint, headers=headers, json=payload, timeout=30).raise_for_status()


def upload_seed_sql_via_linked_cli(schema_sql: Path, seed_sql: Path) -> None:
    if shutil.which("supabase") is None:
        raise RuntimeError("Supabase CLI is not installed or not on PATH.")
    for sql_file in (schema_sql, seed_sql):
        subprocess.run(
            ["supabase", "db", "query", "--linked", "--file", str(sql_file)],
            check=True, cwd=str(SITE_DIR),
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Soft Life pipeline: score → enrich → publish.")
    parser.add_argument("--input",            help="Source CSV path.")
    parser.add_argument("--softlife-output",  default=str(DEFAULT_CSV_OUT),   help="Accepted Soft Life CSV output path.")
    parser.add_argument("--reject-output",    default=str(DEFAULT_CSV_REJECT), help="Rejected CSV output path.")
    parser.add_argument("--site-json",        default=str(DEFAULT_SITE_JSON),  help="Site jobs JSON output path.")
    parser.add_argument("--seed-sql",         default=str(DEFAULT_SEED_SQL),   help="Supabase seed SQL output path.")
    parser.add_argument("--workers",          type=int, default=DEFAULT_WORKERS, help="Concurrent fetch workers.")
    parser.add_argument("--freshness-days",   type=int, default=1,             help="Freshness window for aggregator export.")
    parser.add_argument("--aggregator-dir",   help="Override aggregator checkout path")
    parser.add_argument("--skip-scrape",      action="store_true")
    parser.add_argument("--skip-pull",        action="store_true")
    parser.add_argument("--skip-merge",       action="store_true")
    parser.add_argument("--skip-export",      action="store_true")
    parser.add_argument("--skip-tracker",     action="store_true")
    parser.add_argument("--skip-score",       action="store_true")
    parser.add_argument("--no-cache",         action="store_true")
    parser.add_argument("--upload-supabase",  action="store_true")
    parser.add_argument(
        "--upload-method",
        choices=("auto", "rest", "linked-cli"),
        default="auto",
    )
    parser.add_argument("--supabase-url",         help="Supabase project URL.")
    parser.add_argument("--supabase-service-key", help="Supabase service role key.")
    parser.add_argument("--supabase-table",       default=DEFAULT_SUPABASE_TABLE)
    parser.add_argument(
        "--scrape-source",
        choices=("manual", "automated"),
        default="manual",
    )
    parser.add_argument(
        "--scrape-platform",
        choices=("all", "greenhouse", "ashby", "bamboohr", "lever", "workday", "builtin"),
        default="all",
    )
    parser.add_argument("--scrape-limit", type=int, default=0)
    return parser


def main(argv: Optional[list] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    globals()["AGGREGATOR_DIR"] = AGGREGATOR_DIR
    if args.aggregator_dir:
        globals()["AGGREGATOR_DIR"] = Path(args.aggregator_dir).resolve()
        AGGREGATOR_DIR_OVERRIDE = Path(args.aggregator_dir).resolve()
        globals()["AGGREGATOR_SCRIPTS_DIR"] = AGGREGATOR_DIR_OVERRIDE / "scripts"
        globals()["AGGREGATOR_SCRAPER_SCRIPT"] = AGGREGATOR_DIR_OVERRIDE / "scripts" / "scraper.py"
        globals()["AGGREGATOR_MERGE_SCRIPT"] = AGGREGATOR_DIR_OVERRIDE / "scripts" / "merge_data.py"
        globals()["AGGREGATOR_EXPORT_SCRIPT"] = AGGREGATOR_DIR_OVERRIDE / "scripts" / "export_filtered.py"
        globals()["AGGREGATOR_TRACKER_SCRIPT"] = AGGREGATOR_DIR_OVERRIDE / "scripts" / "track_foreign_drops.py"
        globals()["AGGREGATOR_DATA_DIR"] = AGGREGATOR_DIR_OVERRIDE / "data"
        print(f"Aggregator dir overridden to: {AGGREGATOR_DIR_OVERRIDE}")

    if args.input:
        csv_in = resolve_input_csv(args.input)
        print(f"Source CSV: using explicit input {csv_in}")
    else:
        if args.skip_scrape:
            if not args.skip_pull:
                pull_aggregator_data()
                print("Step 0 complete: pulled latest aggregator data from GitHub")
            else:
                print("Step 0 skipped: scraper not run, git pull skipped")
        else:
            run_scraper(args.scrape_source, args.scrape_platform, args.scrape_limit)
            print("Step 0 complete: scraper refreshed aggregator output")

        if args.skip_merge:
            print("Step 1 skipped: merge_data not run")
        else:
            merge_scraped_jobs()
            print("Step 1 complete: merged scraper output into aggregator data")

        if args.skip_tracker:
            print("Step 1.5 skipped: drop tracker not run")
        else:
            run_drop_tracker()
            print("Step 1.5 complete: updated drop tracker from aggregator data")

        if args.skip_export:
            csv_in = resolve_input_csv(str(DEFAULT_CSV_IN))
            print(f"Step 2 skipped: reusing {csv_in}")
        else:
            csv_in = export_source_csv(DEFAULT_CSV_IN, args.freshness_days)
            print(f"Step 2 complete: exported source CSV to {csv_in}")

    softlife_output = resolve_output_path(args.softlife_output, DEFAULT_CSV_OUT)
    reject_output   = resolve_output_path(args.reject_output,   DEFAULT_CSV_REJECT)
    site_json       = _resolve_path(args.site_json, SITE_DIR)
    schema_sql      = _resolve_path(str(DEFAULT_SCHEMA_SQL), SUPABASE_DIR)
    seed_sql        = _resolve_path(args.seed_sql, SUPABASE_DIR)

    if not args.skip_score:
        summary = _run_pipeline(
            csv_in=csv_in,
            csv_out=softlife_output,
            csv_reject=reject_output,
            workers=args.workers,
            use_cache=not args.no_cache,
        )
        print(f"\nStep 3 complete: {summary['accepted_count']} accepted / {summary['rejected_count']} rejected.")
    else:
        print(f"Step 3 skipped: reusing {softlife_output.name}")

    rows = build_job_records(softlife_output)
    write_jobs_json(rows, site_json)
    print(f"Step 4 complete: wrote {len(rows)} rows to {site_json}")

    write_seed_sql(rows, seed_sql, args.supabase_table)
    print(f"Step 5 prep complete: wrote Supabase seed SQL to {seed_sql}")

    if args.upload_supabase:
        upload_method = args.upload_method
        if upload_method == "auto":
            upload_method = "rest" if (args.supabase_url and args.supabase_service_key) else "linked-cli"

        if upload_method == "rest":
            if not args.supabase_url or not args.supabase_service_key:
                parser.error("--supabase-url and --supabase-service-key are required with --upload-method rest")
            upload_to_supabase(rows, args.supabase_url, args.supabase_service_key, args.supabase_table)
            print(f"Supabase upsert complete: synced {len(rows)} rows to {args.supabase_table} via REST")
        else:
            upload_seed_sql_via_linked_cli(schema_sql, seed_sql)
            print(f"Supabase sync complete: applied schema + seed to linked project via CLI")


if __name__ == "__main__":
    main()
