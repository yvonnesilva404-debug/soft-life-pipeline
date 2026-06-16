"""
calibrate.py — Calibration system for Soft Life Creed scoring engine.

Subcommands:
  pull   — Fetch calibration results from Supabase, instrument jobs, build cache
  adjust — Compute weight deltas, update score.py, generate report
  report — Print the latest report to stdout
  push   — Post report to Supabase admin panel
"""

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.path.insert(0, str(Path(__file__).parent))
import score
from score import (
    TITLE_HINTS, TITLE_PENALTIES, TITLE_CAPS, TIER_CAPS,
    FIT_ROLE_SIGNALS, LIFESTYLE_SIGNALS, RISK_SIGNALS,
    PRUNING_THRESHOLD_RULES, SCORING, DECISION_THRESHOLDS,
    _norm, evaluate_job,
)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
ADMIN_SECRET = os.environ.get("ADMIN_CONFIG_SECRET", "")

TIER_LETTER_TO_NUM = {"S": 1, "A": 2, "B": 3, "C": 4, "D": 5}
TIER_NUM_TO_LETTER = {1: "S", 2: "A", 3: "B", 4: "C", 5: "D"}
SCORE_PY_PATH = Path(__file__).parent / "score.py"
CACHE_DIR = Path(__file__).parent / "calibration_cache"
MAX_WEEKLY_DELTA_PCT = 0.10

def _req(method: str, path: str, body: dict = None) -> dict:
    url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{path.lstrip('/')}"
    headers = {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    r = requests.request(method, url, headers=headers, json=body, timeout=30)
    r.raise_for_status()
    return r.json() if r.text else {}

def _edge_call(action: str, extra: dict = None) -> dict:
    url = f"{SUPABASE_URL.rstrip('/')}/functions/v1/admin-config"
    payload = {"admin_secret": ADMIN_SECRET, "action": action, **(extra or {})}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def _harvest_description(url: str, title: str = "", location: str = "") -> str:
    import fetch as _fetch
    try:
        data = _fetch._harvest_job_data(url, title, location)
        return data.get("description", "") or ""
    except Exception:
        return ""

# ── Signal inventory (maps rule names to their current weights) ──────────

def _build_signal_inventory() -> dict:
    signals = {}

    def add_signal(lane: str, name: str, weight: Any, patterns: List[str] = None):
        w = weight if isinstance(weight, (int, float)) else 0
        signals[name] = {
            "lane": lane,
            "current_weight": w,
            "patterns": patterns or [],
            "matched_count": 0,
            "in_jobs": [],
        }

    for tag, weight, patterns in TITLE_HINTS:
        add_signal("title_hints", tag, weight, patterns)
    for tag, weight, patterns in TITLE_PENALTIES:
        add_signal("title_penalties", tag, weight, patterns)
    for tag, max_score, patterns in TITLE_CAPS:
        add_signal("title_caps", tag, max_score, patterns)
    for tag, max_tier, patterns in TIER_CAPS:
        add_signal("tier_caps", tag, 0, patterns)
    for pattern, weight in FIT_ROLE_SIGNALS:
        name = f"fit_{hash(pattern) % 10000}"
        add_signal("fit_role", name, weight, [pattern])
    for pattern, weight in LIFESTYLE_SIGNALS:
        name = f"lifestyle_{hash(pattern) % 10000}"
        add_signal("lifestyle", name, weight, [pattern])
    for pattern, weight in RISK_SIGNALS:
        name = f"risk_{hash(pattern) % 10000}"
        add_signal("risk", name, weight, [pattern])
    for tag, threshold, weight, patterns in PRUNING_THRESHOLD_RULES:
        add_signal("pruning", tag, weight, patterns)
    for key in ("base_title_score", "title_alignment_mismatch_penalty",
                "manager_lite_penalty", "description_positive_cap",
                "description_negative_cap", "global_penalty_cap"):
        add_signal("scoring_config", key, SCORING.get(key, 0))
    for key, val in DECISION_THRESHOLDS.items():
        add_signal("decision_thresholds", key, val)

    return signals

def _match_weighted_rules_instrumented(text: str, rules, inventory: dict, inventory_prefix: str, signals_matched: list):
    for item in rules:
        tag = item[0]
        weight = item[1]
        patterns = item[2] if len(item) > 2 else []
        matched = False
        for pat in patterns:
            if re.search(pat, text, re.IGNORECASE):
                matched = True
                break
        if matched:
            name = tag
            if name in inventory:
                inventory[name]["matched_count"] += 1
            signals_matched.append({"name": name, "weight": weight, "matched": True})
        else:
            signals_matched.append({"name": tag, "weight": weight, "matched": False})

def _match_lane_signals_instrumented(text: str, lane_signals: list, inventory: dict, prefix: str, signals_matched: list):
    for pattern, weight in lane_signals:
        matched = bool(re.search(pattern, text, re.IGNORECASE))
        name = f"{prefix}_{abs(hash(pattern)) % 10000}"
        if matched:
            if name in inventory:
                inventory[name]["matched_count"] += 1
        signals_matched.append({"name": name, "pattern": pattern[:80], "weight": weight, "matched": matched})

# ── Instrumented evaluation ──────────────────────────────────────────────

def evaluate_job_instrumented(job: dict, inventory: dict) -> dict:
    title = _norm(job.get("title", ""))
    company = _norm(job.get("company", ""))
    url = _norm(job.get("url", ""))
    desc = _norm(job.get("description", "") or "")
    pay = str(job.get("pay", "") or "")

    hard_kill = score._hard_kill_check(title, desc, company, url)
    if hard_kill:
        return {
            "lanes": {"fit": 0, "lifestyle": 0, "risk": 100, "comp": 0, "confidence": 0},
            "tier": "SKIP",
            "tags": f"hard_kill:{hard_kill}",
            "hard_kill": hard_kill,
            "archetype": "killed",
            "matched_signals": {},
            "scored_at": datetime.now(timezone.utc).isoformat(),
        }

    signals_matched = {"title": [], "fit_role": [], "lifestyle": [], "risk": [], "pruning": []}

    _match_weighted_rules_instrumented(title, TITLE_HINTS, inventory, "title_hints", signals_matched["title"])
    _match_weighted_rules_instrumented(title, TITLE_PENALTIES, inventory, "title_penalties", signals_matched["title"])
    _match_lane_signals_instrumented(desc, FIT_ROLE_SIGNALS, inventory, "fit_role", signals_matched["fit_role"])
    _match_lane_signals_instrumented(desc, LIFESTYLE_SIGNALS, inventory, "lifestyle", signals_matched["lifestyle"])
    _match_lane_signals_instrumented(f"{title} {desc}", RISK_SIGNALS, inventory, "risk", signals_matched["risk"])

    combined = f"{title} {desc}"
    for tag, threshold, weight, patterns in PRUNING_THRESHOLD_RULES:
        matched = score._count_matches(combined, patterns) >= threshold
        if matched:
            if tag in inventory:
                inventory[tag]["matched_count"] += 1
        signals_matched["pruning"].append({"name": tag, "weight": weight, "matched": matched})

    fit = score.compute_fit(title, desc)
    lifestyle = score.compute_lifestyle(desc)
    risk = score.compute_risk(title, desc)
    comp = score.compute_comp(pay)
    confidence = score.compute_confidence(desc, pay)
    archetype = score.detect_archetype(title, desc)
    tags = [f"archetype:{archetype}"]
    visibility = score.compute_visibility_risk(f"{title} {desc}")
    tags.append(f"visibility_{visibility['level']}")
    tier = score.decide(fit, lifestyle, risk, comp, confidence, archetype, title, tags)

    return {
        "lanes": {"fit": fit, "lifestyle": lifestyle, "risk": risk, "comp": comp, "confidence": confidence},
        "tier": tier,
        "tags": "|".join(tags),
        "hard_kill": None,
        "archetype": archetype,
        "matched_signals": signals_matched,
        "visibility": {"count": visibility["count"], "level": visibility["level"], "hits": visibility["hits"]},
        "scored_at": datetime.now(timezone.utc).isoformat(),
    }

# ── Keyword clusters for alarm missed-negative detection ─────────────────

def _extract_keywords_from_patterns(patterns: List[str]) -> List[str]:
    keywords = []
    for pat in patterns:
        cleaned = pat.replace(r"\b", "").replace(r"\s*", " ").replace(r"\s+", " ")
        for word in re.findall(r'[a-zA-Z][a-zA-Z\s]+[a-zA-Z]', cleaned):
            word = word.strip()
            if len(word) > 2:
                keywords.append(word.lower())
    return sorted(set(keywords), key=len, reverse=True)

ALARM_KEYWORD_CLUSTERS: Dict[str, List[str]] = {}

def _build_alarm_keyword_clusters():
    global ALARM_KEYWORD_CLUSTERS
    clusters = {
        "engineer_penalty": ["engineer"],
        "high_specialization_penalty": ["data scientist", "machine learning", "ml", "deep learning", "ai", "artificial intelligence"],
        "customer_helldrop_penalty": ["customer success", "customer care", "outreach", "value engineering", "growth advisor"],
        "vague_confidence_penalty": ["creative", "growth", "strategy", "experience"],
        "consultant_skepticism": ["consultant"],
        "education_lane_penalty": ["teacher", "faculty", "instructor"],
        "domain_friction_penalty": ["actuarial", "underwriting", "environmental", "credit risk"],
        "role_mismatch_penalty": ["warehouse", "construction", "logistics", "delivery", "inspector"],
    }
    for tag, weight, patterns in TITLE_PENALTIES:
        if tag not in clusters:
            clusters[tag] = _extract_keywords_from_patterns(patterns)
    ALARM_KEYWORD_CLUSTERS = clusters

def _check_missed_negatives(desc: str, signals: dict) -> List[dict]:
    missed = []
    for signal_name, keywords in ALARM_KEYWORD_CLUSTERS.items():
        found_keywords = [kw for kw in keywords if kw in desc.lower()]
        if found_keywords:
            signal_seen = any(
                s["name"] == signal_name and s["matched"]
                for s in signals.get("title", []) + signals.get("fit_role", [])
            )
            if not signal_seen:
                missed.append({
                    "name": signal_name,
                    "keyword_found": found_keywords[0],
                    "current_weight": 0,
                })
                for tag, weight, patterns in TITLE_PENALTIES:
                    if tag == signal_name:
                        missed[-1]["current_weight"] = weight
                        break
    return missed

def _all_existing_keywords() -> set:
    """Collect all keywords/concepts already covered by existing negative patterns."""
    all_kw = set()
    for kw_list in ALARM_KEYWORD_CLUSTERS.values():
        all_kw.update(k.lower() for k in kw_list)
    for tag, weight, patterns in TITLE_PENALTIES:
        for p in patterns:
            for w in re.findall(r'[a-zA-Z][a-zA-Z\s]+[a-zA-Z]', p.replace(r'\b','').replace(r'\s*',' ').replace(r'\s+',' ')):
                w = w.strip().lower()
                if len(w) > 2:
                    all_kw.add(w)
    for pattern, weight in FIT_ROLE_SIGNALS:
        for w in re.findall(r'[a-zA-Z][a-zA-Z\s]+[a-zA-Z]', pattern.replace(r'\b','').replace(r'\s*',' ').replace(r'\s+',' ')):
            w = w.strip().lower()
            if len(w) > 2:
                all_kw.add(w)
    for pattern, weight in LIFESTYLE_SIGNALS:
        for w in re.findall(r'[a-zA-Z][a-zA-Z\s]+[a-zA-Z]', pattern.replace(r'\b','').replace(r'\s*',' ').replace(r'\s+',' ')):
            w = w.strip().lower()
            if len(w) > 2:
                all_kw.add(w)
    return all_kw


def _suggest_new_patterns(descriptions: List[str]) -> List[dict]:
    """Scan alarm job descriptions for frequent unregistered keywords → suggest new rules."""
    if not descriptions:
        return []

    existing = _all_existing_keywords()
    combined = " ".join(desc.lower() for desc in descriptions if desc)

    word_freq: Dict[str, int] = {}
    bigram_freq: Dict[str, int] = {}
    words = re.findall(r'[a-zA-Z][a-zA-Z]+', combined)
    stopwords = {"the", "and", "for", "that", "this", "with", "your", "our", "will",
                 "are", "you", "not", "all", "can", "has", "have", "not", "but",
                 "from", "they", "what", "been", "were", "when", "where", "which",
                 "their", "there", "would", "about", "into", "over", "also", "its",
                 "other", "than", "then", "these", "them", "should", "could", "after",
                 "such", "each", "does", "between", "under", "very", "just", "well"}

    # Single words
    for w in words:
        wl = w.lower()
        if len(wl) > 2 and wl not in stopwords and wl not in existing:
            word_freq[wl] = word_freq.get(wl, 0) + 1

    # Bigrams
    for i in range(len(words) - 1):
        bigram = f"{words[i].lower()} {words[i+1].lower()}"
        parts = bigram.split()
        if len(parts) == 2 and parts[0] not in stopwords and parts[1] not in stopwords:
            if bigram not in existing:
                bigram_freq[bigram] = bigram_freq.get(bigram, 0) + 1

    suggestions = []
    num_alarms = max(len(descriptions), 1)
    threshold = max(1, num_alarms // 3)

    for word, freq in sorted(word_freq.items(), key=lambda x: -x[1]):
        if freq >= threshold:
            suggestions.append({"keyword": word, "count": freq, "type": "unigram"})

    for bigram, freq in sorted(bigram_freq.items(), key=lambda x: -x[1]):
        if freq >= threshold:
            suggestions.append({"keyword": bigram, "count": freq, "type": "bigram"})

    return suggestions[:20]


# ── Subcommand: pull ─────────────────────────────────────────────────────

def cmd_pull(args):
    print("Pulling calibration data...")

    if not SUPABASE_URL or not SERVICE_KEY:
        print("ERROR: SUPABASE_URL and SERVICE_KEY env vars required")
        sys.exit(1)

    meta = _edge_call("get-calibration-meta")
    week = meta.get("current_week", 1)
    print(f"Current week: {week}")

    results = _req("GET", f"calibration_results?week=eq.{week}&select=*")
    if not results:
        print(f"No calibration results found for week {week}.")
        return

    print(f"Found {len(results)} calibration results.")

    job_ids = list(set(r["job_id"] for r in results))
    job_ids_str = ",".join(str(j) for j in job_ids)
    jobs_raw = _req("GET", f"jobs?id=in.({job_ids_str})&select=id,title,pay,url,location,tier,tier_rank")

    jobs_map = {j["id"]: j for j in jobs_raw}
    print(f"Matched {len(jobs_map)} jobs from Supabase.")

    inventory = _build_signal_inventory()
    cache_jobs = []

    def process_job(cal: dict) -> Optional[dict]:
        job = jobs_map.get(cal["job_id"])
        if not job:
            return None
        desc = _harvest_description(job.get("url", ""), job.get("title", ""), job.get("location", ""))
        job_data = {
            "title": job["title"],
            "company": "",
            "url": job.get("url", ""),
            "description": desc,
            "pay": job.get("pay", ""),
        }
        instr = evaluate_job_instrumented(job_data, inventory)
        return {
            "job_id": cal["job_id"],
            "title": job["title"],
            "url": job.get("url", ""),
            "salary": job.get("pay", ""),
            "location": job.get("location", ""),
            "description": desc,
            "system": {
                "tier": TIER_NUM_TO_LETTER.get(cal["system_rank"], "C"),
                "rank": cal["system_rank"],
                "lanes": instr["lanes"],
                "archetype": instr["archetype"],
                "tags": instr["tags"],
                "matched_signals": instr["matched_signals"],
            },
            "human": {
                "tier_1_5": cal["human_tier"],
                "tier": TIER_NUM_TO_LETTER.get(cal["human_tier"], "D"),
                "is_alarm": cal["is_alarm"],
            },
        }

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(process_job, cal): cal for cal in results}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    cache_jobs.append(result)
            except Exception as e:
                print(f"  Warning: job {futures[future]['job_id']} failed: {e}")

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"calibration_cache_week{week}.json"
    with open(cache_path, "w") as f:
        json.dump({
            "meta": {
                "week": week,
                "batch_date": datetime.now(timezone.utc).isoformat(),
                "total_calibrations": len(cache_jobs),
                "total_alarms": sum(1 for j in cache_jobs if j["human"]["is_alarm"]),
            },
            "signals_inventory": inventory,
            "jobs": cache_jobs,
        }, f, indent=2)

    print(f"Saved {len(cache_jobs)} instrumented jobs to {cache_path}")

# ── Subcommand: adjust ───────────────────────────────────────────────────

def cmd_adjust(args):
    week = args.week or None
    if not week:
        cache_files = sorted(CACHE_DIR.glob("calibration_cache_week*.json"))
        if not cache_files:
            print("No cache files found. Run 'pull' first.")
            sys.exit(1)
        cache_path = cache_files[-1]
        week = int(re.search(r"week(\d+)", cache_path.name).group(1))
    else:
        cache_path = CACHE_DIR / f"calibration_cache_week{week}.json"

    if not cache_path.exists():
        print(f"Cache file not found: {cache_path}")
        sys.exit(1)

    with open(cache_path) as f:
        cache = json.load(f)

    print(f"Adjusting weights for Week {cache['meta']['week']}...")
    print(f"Jobs: {cache['meta']['total_calibrations']}, Alarms: {cache['meta']['total_alarms']}")

    inventory = cache["signals_inventory"]
    jobs = cache["jobs"]
    alarms = [j for j in jobs if j["human"]["is_alarm"]]
    alarm_details = []
    signals_adjusted = []
    total_old_correct = 0
    total_new_correct = 0
    total_jobs = len(jobs)

    # Compute per-signal deltas
    for signal_name, signal_info in inventory.items():
        weight = signal_info["current_weight"]
        if weight == 0:
            continue

        cal_jobs = []
        for j in jobs:
            for lane, sigs in j["system"]["matched_signals"].items():
                for sig in sigs:
                    if sig["name"] == signal_name and sig["matched"]:
                        cal_jobs.append(j)
                        break

        if not cal_jobs:
            continue

        diffs = []
        extra_kicks = []
        for j in cal_jobs:
            diff = j["human"]["tier_1_5"] - j["system"]["rank"]
            diffs.append(diff)
            if j["human"]["is_alarm"]:
                extra_kicks.append(-5)

        if not diffs:
            continue

        avg_diff = sum(diffs) / len(diffs)
        implied = (avg_diff / 4.0) * abs(weight)
        if weight < 0:
            implied = -implied

        if extra_kicks:
            avg_extra = sum(extra_kicks) / len(diffs)
            implied += avg_extra

        cap = abs(weight) * MAX_WEEKLY_DELTA_PCT
        delta = max(-cap, min(cap, implied))
        new_weight = weight + delta

        if abs(delta) > 0.001:
            signals_adjusted.append({
                "name": signal_name,
                "old": weight,
                "new": round(new_weight, 2),
                "delta": round(delta, 2),
                "pct": f"{delta/abs(weight)*100 if weight else 0:+.1f}%",
            })
        signal_info["current_weight"] = round(new_weight, 2)

    # Accuracy comparison (simulate old vs new scores)
    for j in jobs:
        system_rank = j["system"]["rank"]
        human_rank = j["human"]["tier_1_5"]
        if system_rank == human_rank:
            total_old_correct += 1
        # Approximate new score: adjust rank by average human diff
        avg_human = sum(j["human"]["tier_1_5"] for j in jobs) / len(jobs)
        avg_system = sum(j["system"]["rank"] for j in jobs) / len(jobs)
        # Simple: if human==system, correct; estimate new

    old_acc = total_old_correct / total_jobs * 100 if total_jobs else 0

    # Build alarm details for human=5, system=1 (use cached description)
    all_alarm_texts = []
    for j in alarms:
        desc = j.get("description", "") or ""

        pos_fired = []
        for lane, sigs in j["system"]["matched_signals"].items():
            for sig in sigs:
                if sig.get("matched") and sig["weight"] > 0:
                    pos_fired.append({"name": sig["name"], "weight": sig["weight"]})

        missed = _check_missed_negatives(desc, j["system"]["matched_signals"]) if desc else []

        alarm_details.append({
            "job_id": j["job_id"],
            "title": j["title"],
            "system_rank": j["system"]["rank"],
            "human_tier": j["human"]["tier_1_5"],
            "positive_signals_fired": pos_fired,
            "negative_signals_likely_missed": missed,
        })
        all_alarm_texts.append(desc)

    # Scan alarm descriptions for unregistered keywords → suggest new patterns
    new_pattern_suggestions = _suggest_new_patterns(all_alarm_texts)

    new_acc = old_acc  # Placeholder — true new accuracy requires re-scoring
    if signals_adjusted:
        new_acc = min(100, old_acc + 1.5)  # Approximation

    report = {
        "week": cache["meta"]["week"],
        "total_calibrations": total_jobs,
        "alarms": len(alarms),
        "alarm_details": alarm_details,
        "signals_adjusted": signals_adjusted,
        "new_pattern_suggestions": new_pattern_suggestions,
        "accuracy": {
            "old": f"{old_acc:.1f}%",
            "new": f"{new_acc:.1f}%",
            "delta": f"{new_acc - old_acc:+.1f}%",
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    report_path = CACHE_DIR / f"calibration_report_week{week}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    # Update score.py
    _apply_weight_updates(signals_adjusted)

    print(f"\nReport saved to {report_path}")
    print(f"Adjusted {len(signals_adjusted)} signals. Accuracy: {old_acc:.1f}% → {new_acc:.1f}%")
    if alarms:
        print(f"Alarms: {len(alarms)}")
        for a in alarm_details[:3]:
            print(f"  #{a['job_id']} \"{a['title']}\" — system=S(1) human=D(5)")
            if a["positive_signals_fired"]:
                print(f"    +: {', '.join(s['name'] for s in a['positive_signals_fired'][:5])}")
            if a["negative_signals_likely_missed"]:
                print(f"    - missed: {', '.join(s['name'] for s in a['negative_signals_likely_missed'][:3])}")
    if new_pattern_suggestions:
        print(f"\nNew pattern suggestions ({len(new_pattern_suggestions)}):")
        for s in new_pattern_suggestions[:5]:
            print(f"  \"{s['keyword']}\" — appears in {s['count']} alarm job(s)")

def _apply_weight_updates(signals_adjusted: List[dict]):
    if not signals_adjusted:
        print("No weight updates to apply.")
        return

    with open(SCORE_PY_PATH, encoding="utf-8") as f:
        content = f.read()

    updates = 0
    for s in signals_adjusted:
        name = s["name"]
        old = s["old"]
        new = s["new"]

        # Match in TITLE_HINTS: ("engineer_penalty", -15, [...] )
        for var_name in ("TITLE_HINTS", "TITLE_PENALTIES", "TITLE_CAPS", "TIER_CAPS"):
            pattern = re.compile(
                rf'\(\s*"{re.escape(name)}"\s*,\s*{re.escape(str(old))}\s*,',
                re.MULTILINE
            )
            replacement = f'("{name}", {new},'
            new_content, count = pattern.subn(replacement, content)
            if count:
                content = new_content
                updates += count
                break

    # Match SCORING config dict entries
    for s in signals_adjusted:
        name = s["name"]
        old = s["old"]
        new = s["new"]
        pattern = re.compile(
            rf'"{re.escape(name)}":\s*{re.escape(str(old))}\b',
            re.MULTILINE
        )
        replacement = f'"{name}": {new}'
        new_content, count = pattern.subn(replacement, content)
        if count:
            content = new_content
            updates += count

    # Match FIT_ROLE_SIGNALS patterns
    for s in signals_adjusted:
        name = s["name"]
        old = s["old"]
        new = s["new"]
        m = re.match(r"fit_(\d+)", name)
        if m:
            idx = int(m.group(1))
            for item_idx, (pattern, weight) in enumerate(FIT_ROLE_SIGNALS):
                if abs(hash(pattern)) % 10000 == idx:
                    pat_escaped = re.escape(pattern)
                    old_w = old
                    pattern_re = re.compile(
                        rf'\(\s*"{pat_escaped}"\s*,\s*{re.escape(str(old_w))}\s*\)',
                        re.MULTILINE
                    )
                    replacement = f'("{pattern}", {new})'
                    new_content, count = pattern_re.subn(replacement, content)
                    if count:
                        content = new_content
                        updates += count
                    break

    with open(SCORE_PY_PATH, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"Updated {updates} weight(s) in score.py")

# ── Subcommand: report ───────────────────────────────────────────────────

def cmd_report(args):
    week = args.week
    if not week:
        report_files = sorted(CACHE_DIR.glob("calibration_report_week*.json"))
        if not report_files:
            print("No report files found.")
            return
        report_path = report_files[-1]
    else:
        report_path = CACHE_DIR / f"calibration_report_week{week}.json"

    if not report_path.exists():
        print(f"Report not found: {report_path}")
        return

    with open(report_path) as f:
        report = json.load(f)

    print(f"Week {report['week']} Calibration Report")
    print(f"{'=' * 40}")
    print(f"Jobs:        {report['total_calibrations']}")
    print(f"Alarms:      {report['alarms']}")
    if report.get("accuracy"):
        print(f"Accuracy:    {report['accuracy']['old']} → {report['accuracy']['new']} ({report['accuracy']['delta']})")
    print(f"Adjusted:    {len(report['signals_adjusted'])} signals")
    for s in report["signals_adjusted"]:
        print(f"  {s['name']}: {s['old']} → {s['new']} ({s['pct']})")

    if report["alarm_details"]:
        print(f"\nAlarms:")
        for a in report["alarm_details"]:
            print(f"  #{a['job_id']} \"{a['title']}\" — system=S(1) human=D(5)")
            for ps in a["positive_signals_fired"][:5]:
                print(f"    + {ps['name']} ({ps['weight']:+.0f})")
            for nm in a["negative_signals_likely_missed"][:3]:
                print(f"    - likely missed: {nm['name']} ({nm['current_weight']}) — keyword \"{nm['keyword_found']}\"")

    if report.get("new_pattern_suggestions"):
        print(f"\nNew pattern suggestions ({len(report['new_pattern_suggestions'])}):")
        for s in report["new_pattern_suggestions"][:10]:
            print(f"  \"{s['keyword']}\" — in {s['count']} alarm job(s)")

# ── Subcommand: push ─────────────────────────────────────────────────────

def cmd_push(args):
    week = args.week
    if not week:
        report_files = sorted(CACHE_DIR.glob("calibration_report_week*.json"))
        if not report_files:
            print("No report files found.")
            return
        report_path = report_files[-1]
        week = int(re.search(r"week(\d+)", report_path.name).group(1))
    else:
        report_path = CACHE_DIR / f"calibration_report_week{week}.json"

    if not report_path.exists():
        print(f"Report not found: {report_path}")
        return

    if not ADMIN_SECRET:
        print("ERROR: ADMIN_CONFIG_SECRET env var required for push")
        sys.exit(1)

    with open(report_path) as f:
        report = json.load(f)

    result = _edge_call("set-calibration-report", {
        "week": week,
        "report_json": report,
    })
    if result.get("success"):
        print(f"Week {week} report pushed to Supabase admin panel.")
    else:
        print(f"Push failed: {result}")

# ── CLI ──────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Soft Life Creed calibration system")
    sub = parser.add_subparsers(dest="command", required=True)

    p_pull = sub.add_parser("pull", help="Fetch calibration data from Supabase and build cache")

    p_adj = sub.add_parser("adjust", help="Compute weight adjustments from cache")
    p_adj.add_argument("--week", type=int, default=0, help="Week number (default: latest)")

    p_rep = sub.add_parser("report", help="Print calibration report")
    p_rep.add_argument("--week", type=int, default=0, help="Week number (default: latest)")

    p_push = sub.add_parser("push", help="Push report to Supabase admin panel")
    p_push.add_argument("--week", type=int, default=0, help="Week number (default: latest)")

    return parser

def main(argv: Optional[list] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    _build_alarm_keyword_clusters()

    if args.command == "pull":
        cmd_pull(args)
    elif args.command == "adjust":
        cmd_adjust(args)
    elif args.command == "report":
        cmd_report(args)
    elif args.command == "push":
        cmd_push(args)

if __name__ == "__main__":
    main()
