"""
score.py — Multi-lane scoring engine, pay filtering, and scoring config.

Merged from: score_soft/config.py + score_soft/scorer.py + harvest_soft/filters.py

Architecture: five scoring lanes (fit, lifestyle, risk, comp, confidence) feed a
decision engine rather than accumulating into a single integer.

Public API:
    evaluate_job(job: dict) -> dict
    harvest_gate(job: dict) -> str | None
"""

import os
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------
PatternRule = Tuple[str, List[str], bool]
WeightedSignal = Tuple[str, int, List[str]]
ThresholdRule = Tuple[str, int, int, List[str]]
HardKillThresholdRule = Tuple[str, int, List[str]]
LaneSignal = Tuple[str, int]  # (pattern, weight) — used in lane signal lists

# ============================================================================
# SECTION 1 — Pay extraction (from harvest_soft/filters.py)
# ============================================================================

_PAY_RE = re.compile(
    r"(?:\b[A-Z]{3}\b\s*)?"
    r"(?:\$|€|£)?\s*((?:(?:\d{1,3}(?:,\d{3})+)|(?:\d+))(?:\.\d+)?)\s*"
    r"(?:[-\u2010\u2011\u2012\u2013\u2014\u2015]\s*(?:\$|€|£)?\s*((?:(?:\d{1,3}(?:,\d{3})+)|(?:\d+))(?:\.\d+)?))?\s*"
    r"(?:\s*(?:[A-Z]{3}|[A-Z]{3}\.)\s*)?"
    r"(?:\s*(?:/|per\s+)?(year|yr|annual|hour|hr|hourly|month|mo|monthly|week|wk|weekly|day|daily))?",
    re.IGNORECASE,
)

_K_RE = re.compile(r"(\d{2,4})[kK]")

_NON_FULLTIME_TERMS = {
    "part-time", "part time", "parttime",
    "contract", "contractor", "freelance",
    "temporary", "temp", "seasonal",
    "intern", "internship", "co-op", "coop", "externship",
    "per diem", "casual", "on-call",
}


def _normalize_amount(s: str) -> int:
    return int(float(s.replace(",", "")))


def _extract_pay_range(text: str) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    if not text:
        return None, None, None

    text = re.sub(r"\b(?:401k|403b)\b", "", text, flags=re.IGNORECASE)
    text = _K_RE.sub(lambda m: str(int(m.group(1)) * 1000), text)

    for match in _PAY_RE.finditer(text):
        low_s = match.group(1)
        high_s = match.group(2)
        unit_s = (match.group(3) or "").lower()

        try:
            low = _normalize_amount(low_s)
        except Exception:
            continue

        high = None
        if high_s:
            try:
                high = _normalize_amount(high_s)
            except Exception:
                high = None

        if not unit_s:
            if low >= 10000:
                unit_s = "year"
            elif low >= 15:
                unit_s = "hour"
            else:
                continue

        if high is not None and high < low:
            return None, None, None
        if unit_s in ("hour", "hr", "hourly"):
            if low > 2000 or (high is not None and high > 2000):
                continue
            return low, high, "hour"
        if unit_s in ("day", "daily"):
            if low * 12 > 10000000:
                continue
            return int(low / 8), None, "hour"
        if unit_s in ("week", "wk", "weekly"):
            if low * 52 > 10000000:
                continue
            return low * 52, None, "year"
        if unit_s in ("month", "mo", "monthly"):
            if low * 12 > 10000000:
                continue
            return low * 12, None, "year"
        if unit_s in ("year", "yr", "annual"):
            if high is not None and high < 20000:
                if low * 12 > 10000000 or high * 12 > 10000000:
                    continue
                return int(low * 12), int(high * 12), "year"
            if low > 10000000 or (high is not None and high > 10000000):
                continue
            return low, high, "year"

    return None, None, None


def _extract_pay(text: str) -> Tuple[Optional[int], Optional[str]]:
    low, _, unit = _extract_pay_range(text)
    return low, unit


def _is_low_pay(pay_text: str) -> bool:
    low, high, unit = _extract_pay_range(pay_text)
    if low is None:
        return False

    hard_annual = int(os.getenv("LOW_PAY_HARD_SKIP_ANNUAL", "50000"))
    hard_hourly = int(os.getenv("LOW_PAY_HARD_SKIP_HOURLY", "25"))

    if unit == "year":
        if high is not None and high >= hard_annual:
            return False
        return low < hard_annual
    if unit == "hour":
        if high is not None and high >= hard_hourly:
            return False
        return low < hard_hourly
    return False


def _has_pay(pay_text: str) -> bool:
    if not pay_text or not isinstance(pay_text, str):
        return False
    normalized = re.sub(r"\b(?:401k|403b|401\(k\)|403\(b\))\b", "", pay_text.strip(), flags=re.IGNORECASE).strip().lower()
    low, unit = _extract_pay(normalized)
    if low is not None and unit is not None:
        return True
    if not normalized:
        return False
    if normalized in {"competitive", "doe", "tbd", "negotiable", "dependent on experience", "market rate"}:
        return True
    if any(tok in normalized for tok in ("$", "usd", "eur", "£", "k", "hour", "year", "annually")):
        return True
    if re.fullmatch(r"\$?\d[\d,]*(?:[kK])?", normalized):
        return True
    return False


def _is_non_fulltime(employment_type: str) -> bool:
    text = str(employment_type or "").lower().strip()
    if not text:
        return False
    return any(term in text for term in _NON_FULLTIME_TERMS)


def harvest_gate(job: dict) -> Optional[str]:
    """Post-source gate: returns a rejection reason string or None if the job passes."""
    pay = str(job.get("pay", "") or "").strip()

    if not pay:
        return "missing_pay"
    normalized_pay = re.sub(r"\b(?:401k|403b|401\(k\)|403\(b\))\b", "", pay, flags=re.IGNORECASE).strip()
    if re.search(r"\b(?:401k|403b)\b", pay.lower()) and not _has_pay(normalized_pay):
        return "missing_pay"
    if int(os.getenv("FILTER_LOW_PAY", "1")) and _is_low_pay(pay):
        return "low_pay"
    if int(os.getenv("FILTER_MISSING_PAY", "0")) and not _has_pay(pay):
        return "missing_pay"
    if int(os.getenv("FILTER_FULLTIME_ONLY", "0")):
        employment_type = str(job.get("employment_type", "") or "")
        if _is_non_fulltime(employment_type):
            return "not_fulltime"
    return None


# ============================================================================
# SECTION 2 — Scoring config (from score_soft/config.py)
# ============================================================================

SHORT_DESCRIPTION_THRESHOLD = 20
SHORT_DESCRIPTION_PENALTY = -15

VISIBILITY_TERMS = [
    "present",
    "presentation",
    "stakeholder",
    "facilitate",
    "host meetings",
    "run meetings",
    "drive alignment",
    "cross-functional",
]

VISIBILITY_THRESHOLDS = {
    "low": 1,
    "medium": 3,
    "high": 5,
}

VISIBILITY_PENALTIES = {
    "medium": -12,
    "high": -30,
}

HARD_KILLS: List[PatternRule] = [
    ("cold_calling", [
        r"\bcold\s*call(ing)?\b",
        r"\byou\s*will\b.{0,40}\bprospect\b",
        r"\byour\s*role\b.{0,40}\bprospect\b",
        r"\bresponsib\w+\b.{0,40}\bprospect(ing)?\b",
    ], False),
    ("mlm", [
        r"\bmulti[-\s]?level\s*marketing\b",
        r"\bMLM\b",
        r"\bnetwork\s*marketing\b",
        r"\bbuild\s*your\s*(own\s*)?(team|network|downline)\b",
    ], False),
    ("crypto_web3", [
        r"\bcrypto\b",
        r"\bweb\s*3\b",
        r"\bNFT\b",
        r"\bDeFi\b",
        r"\bblockchain\s*(engineer|developer|architect|analyst|specialist)\b",
        r"\bsmart\s*contract\b",
        r"\btoken(omics|ization)?\s*(engineer|developer|analyst)\b",
        r"\bsolidity\b",
    ], True),
    ("real_estate", [
        r"\breal\s*estate\s*(agent|broker|sales)\b",
        r"\brealtor\b",
        r"\bmortgage\s*(broker|loan\s*officer)\b",
    ], False),
    ("legal_role", [
        r"\battorney\b",
        r"\bcounsel\b",
        r"\bparalegal\b",
        r"\blegal\s*(advisor|associate|director|manager|officer)\b",
        r"\bgeneral\s*counsel\b",
        r"\bin[-\s]?house\s*(counsel|attorney|lawyer)\b",
        r"\blawyer\b",
    ], True),
    ("claims_litigation", [
        r"\bclaims?\b",
        r"\blitigation\b",
        r"\binsurance\s*adjuster\b",
        r"\bclaims?\s*adjuster\b",
        r"\bclaims?\s*examiner\b",
        r"\bclaim\s*specialist\b",
        r"\bworkers'?s\s*comp\b",
    ], False),
    ("license_or_board_certified", [
        r"\bstate\s+board\b",
        r"\bboard\s+certified\b",
        r"\bboard\s+certification\b",
        r"\bPMHNP\b",
        r"\bLCSW\b",
        r"\bLPCC\b",
        r"\bLMFT\b",
        r"\blicensed\s+clinical\s+social\s+worker\b",
        r"\blicensed\s+professional\s+clinical\s+counselor\b",
        r"\blicensed\s+marriage\s+and\s+family\s+therapist\b",
        r"\bmental\s+health\s+therapist\b",
        r"\blicensed\s+practical\s+nurse\b",
        r"\bregistered\s+nurse\b",
        r"\blicensed\s+nurse\b",
        r"\bLPN\b",
        r"\bRN\b",
        r"\bphysician\b",
    ], False),
    ("trades_role", [
        r"\bbus\s*driver\b",
        r"\bCDL\b",
        r"\btruck\s*driver\b",
        r"\bmechanic\b",
        r"\belectrician\b",
        r"\bplumber\b",
        r"\bweldr?(er|ing)\b",
        r"\bcarpenter\b",
        r"\bHVAC\b",
        r"\bfield\s*service\s*technician\b",
        r"\bmaintenance\s*technician\b",
        r"\bfacilities\s*technician\b",
        r"\bcrane\s*operator\b",
        r"\brigger\b",
        r"\bforklift\b",
        r"\bdiesel\s*mechanic\b",
        r"\bsolar\s*(installer|technician|electrician)\b",
        r"\blow\s*voltage\s*technician\b",
        r"\bcommissioning\s*technician\b",
    ], True),
    ("defense_engineering", [
        r"\bnuclear\s*(engineer|operator|technician|specialist)\b",
        r"\bnuclear\s+engineering\b",
        r"\breactor\s*(operator|engineer)\b",
        r"\bnuclide\b",
        r"\baerospace\s*(engineer|technician)\b",
        r"\bflight\s*(software|hardware|systems)\s*engineer\b",
        r"\bGNC\s*engineer\b",
        r"\bsatellite\s*(engineer|systems)\b",
        r"\bFPGA\s*(engineer|designer)\b",
        r"\bembedded\s*(systems|firmware)\s*engineer\b",
        r"\bdefense\s*(engineer|systems|contractor)\b",
        r"\bmission\s*systems\s*engineer\b",
        r"\bpropulsion\s*engineer\b",
        r"\bavionics\b",
    ], True),
    ("phone_heavy", [
        r"\bphone\s*(support|based\s*role)\b",
        r"\binbound\s*(calls?|phone\s*calls?)\b",
        r"\bcall\s*center\b",
        r"\bhigh\s*volume\s*calls?\b",
        r"\b\d{2,3}\+?\s*calls?\s*(per\s*day|daily|a\s*day)\b",
        r"\banswering\s*(phones?|calls?)\s*(all\s*day|daily|throughout)\b",
        r"\bhotline\s*(operator|agent|representative)\b",
    ], False),
    ("advanced_degree_requirement", [
        r"\b(master'?s|m\.s\.?|ms|mba|ph\.?d|doctoral)\b.{0,40}\b(required|preferred|or\s*equivalent)\b",
        r"\b(required|preferred|or\s*equivalent)\b.{0,40}\b(master'?s|m\.s\.?|ms|mba|ph\.?d|doctoral)\b",
    ], False),
    ("on_site_required", [
        r"\b(must|required|mandatory)\b.{0,30}\b(on[-\s]?site|in[-\s]?office|in[-\s]?person)\b",
        r"\brelocation\s*(required)\b",
        r"\bwill\s*not\s*consider\s*remote\b",
        r"\bno\s*remote\b",
        r"\bnot\s+(a\s+)?work[-\s]?from[-\s]?home\b",
        r"\bwork[-\s]?from[-\s]?home\b.{0,30}\bnot\b",
        r"\b(should|must)\s+reside\s+in\b",
    ], False),
    ("hybrid_on_site_required", [
        r"\bhybrid\b.{0,80}\b(on[-\s]?site|onsite|in[-\s]?office|office|in[-\s]?person)\b",
        r"\b(on[-\s]?site|onsite|in[-\s]?office|office|in[-\s]?person)\b.{0,80}\bhybrid\b",
    ], False),
    ("staffing_agency", [
        r"\bstaffing\s*(agency|firm|company)\b",
        r"\bw2\s*contract\b",
        r"\bc2c\b",
        r"\brecruiting\s+on\s+behalf\s+of\s+(our\s+)?(client|company)\b",
        r"\bour\s+client\s+is\s+(looking|seeking|hiring)\b",
        r"\bplaced\s+(at|with)\s+(our\s+)?client\b",
    ], False),
]

HARD_KILL_THRESHOLD_RULES: List[HardKillThresholdRule] = []

TITLE_HINTS: List[WeightedSignal] = [
    ("operations", 2, [r"\boperations?\b", r"\bops\b"]),
    ("implementation_onboarding", 5, [r"\bimplementation\b", r"\bonboarding\b"]),
    ("data_analyst_title", 8, [r"\bdata\s+analyst\b"]),
    ("ops_analyst_title", 8, [r"\boperations?\s+analyst\b"]),
    ("reporting_analyst_title", 8, [r"\breporting\s+analyst\b"]),
    ("compliance_analyst_title", 6, [r"\bcompliance\s+analyst\b"]),
    ("risk_analyst_title", 6, [r"\brisk\s+analyst\b"]),
    ("process_analyst_title", 6, [r"\bprocess\s+analyst\b"]),
    ("quality_analyst_title", 6, [r"\bquality\s+(analyst|assurance)\b"]),
    ("it_support_title", 6, [
        r"\bIT\s+(analyst|support|specialist|coordinator)\b",
        r"\bsystems?\s+analyst\b",
        r"\bbusiness\s+systems?\s+analyst\b",
    ]),
    ("edi_title", 10, [r"\bEDI\b"]),
    ("process_in_title", 4, [r"\bprocess\b"]),
    ("reporting_in_title", 4, [r"\breporting\b"]),
]

TITLE_METADATA_TAGS: List[Tuple[str, List[str]]] = [
    ("analyst", [r"\banalyst\b"]),
    ("data", [r"\bdata\b"]),
    ("compliance", [r"\bcompliance\b"]),
    ("risk", [r"\brisk\b"]),
    ("remote", [r"\bremote\b"]),
    ("it_support", [r"\bIT\s+(analyst|support|specialist)\b", r"\bsystems?\s+analyst\b"]),
    ("edi", [r"\bEDI\b"]),
    ("process", [r"\bprocess\b"]),
    ("quality_analyst", [r"\bquality\s+(analyst|assurance)\b"]),
    ("risk_analyst", [r"\brisk\s+analyst\b"]),
    ("compliance_analyst", [r"\bcompliance\s+analyst\b"]),
]

TITLE_PENALTIES: List[WeightedSignal] = [
    ("soft_risk_coordinator", -5, [r"\bcoordinator\b"]),
    ("soft_risk_specialist", -5, [r"\bspecialist\b"]),
    ("bad_domain_specialist", -8, [
        r"\bsales\s*(operations\s*)?(specialist|coordinator)\b",
        r"\bclinical\s*(specialist|coordinator)\b",
        r"\bfield\s*service\s*specialist\b",
        r"\bcoding\s*specialist\b",
        r"\bcustomer\s*(care|success)\s*specialist\b",
        r"\barea\s*\w+\s*specialist\b",
    ]),
    ("soft_risk_assistant", -12, [r"\bassistant\b"]),
    ("soft_risk_representative", -12, [r"\brepresentative\b"]),
    ("role_mismatch_penalty", -12, [
        r"\bpharmacy\s*technician\b",
        r"\bmobile\s*vehicle\s*inspector\b",
        r"\bfield\s*title\s*searcher\b",
        r"\b(warehouse|construction|logistics|delivery|inspector)\b",
    ]),
    ("high_specialization_penalty", -10, [
        r"\bdata\s*scientist\b",
        r"\bmachine\s*learning\b",
        r"\bML\b",
        r"\bdeep\s*learning\b",
        r"\bAI\b",
    ]),
    ("domain_friction_penalty", -8, [
        r"\bactuarial\b",
        r"\bunderwriting\b",
        r"\benvironmental\b",
        r"\bcredit\s+risk\b",
        r"\bcredit\s*analyst\b",
    ]),
    ("consultant_skepticism", -10, [r"\bconsultant\b"]),
    ("education_lane_penalty", -20, [r"\bteacher\b", r"\bfaculty\b", r"\binstructor\b"]),
    ("customer_helldrop_penalty", -15, [
        r"\bcustomer\s*care\b",
        r"\boutreach\s*agent\b",
        r"\bretail\s*(?:cx|agent|customer)\b",
        r"\bcx\s*retail\b",
        r"\bcustomer\s*success\b",
        r"\bvalue\s*engineering\b",
        r"\bgrowth\s*advisor\b",
        r"\bstrategic\s*partner\b",
    ]),
    ("manager_penalty", -2, [r"\bmanager\b"]),
    ("lead_principal_penalty", -8, [r"\b(lead|principal)\b"]),
    ("architect_penalty", -5, [r"\barchitect\b"]),
    ("engineer_penalty", -15, [r"\bengineer\b"]),
    ("leader_penalty", -8, [r"\bleader\b", r"\bdirector\b", r"\bvp\b", r"\bhead\b"]),
    ("vague_confidence_penalty", -10, [r"\bcreative\b", r"\bgrowth\b", r"\bstrategy\b", r"\bexperience\b"]),
]

TITLE_CAPS: List[Tuple[str, int, List[str]]] = [
    ("contractor_cap", 80, [r"\bcontractor\b"]),
    ("engineer_title_cap", 55, [r"\bengineer\b"]),
    ("customer_helldrop_penalty", 90, [
        r"\bcustomer\s*care\b",
        r"\boutreach\s*agent\b",
        r"\bretail\s*(?:cx|agent|customer)\b",
        r"\bcx\s*retail\b",
        r"\bcustomer\s*success\b",
        r"\bvalue\s*engineering\b",
        r"\bgrowth\s*advisor\b",
        r"\bstrategic\s*partner\b",
    ]),
]

TIER_CAPS: List[Tuple[str, str, List[str]]] = [
    ("too_senior", "MAYBE", [
        r"\b(VP|vice\s*president)\b",
        r"\bchief\s*(of|technology|product|revenue|operating|financial|information|commercial|data)\b",
        r"\bC[TSP]O\b",
        r"\bCEO\b",
        r"\bCFO\b",
        r"\bCOO\b",
        r"\bCIO\b",
        r"\bCISO\b",
        r"\bmanaging\s*partner\b",
        r"\bequity\s*partner\b",
        r"\bsenior\s*vice\s*president\b",
        r"\bsvp\b",
        r"\bevp\b",
        r"\bdirector\b",
        r"\bhead\s*of\b",
    ]),
    ("sales_marketing", "MAYBE", [
        r"\bsales\b",
        r"\bmarketing\b",
        r"\bgrowth\s*(advisor|manager|specialist|operations|partner)\b",
        r"\baccount\s*(executive|manager)\b",
        r"\bbusiness\s*development\b",
        r"\bbrand\b",
        r"\badvertis",
    ]),
    ("commission_quota", "REVIEW", [
        r"\bcommission\b",
        r"\bquota\b",
        r"\bOTE\b",
        r"\bcold\s*call\b",
        r"\bupsell\b",
        r"\bbook\s*of\s*business\b",
    ]),
    ("fast_paced", "SKIP", [
        r"\bfast(?:[-\s]?(?:pace|paced))\b",
        r"\bfast[-\s]?moving\b",
        r"\bmove\s+fast\b",
        r"\bhigh[-\s]?pressure\b",
    ]),
]

# ============================================================================
# SECTION 2C — Fit lane: description role signals
# ============================================================================

FIT_ROLE_SIGNALS: List[LaneSignal] = [
    # --- role type positives ---
    (r"\bbusiness\s+systems?\s+analyst\b", 25),
    (r"\bsystems?\s+analyst\b", 15),
    (r"\boperations?\s+analyst\b", 15),
    (r"\bcompliance\s+analyst\b", 20),
    (r"\brisk\s+analyst\b", 20),
    (r"\bqa\s*analyst\b", 20),
    (r"\bdata\s+analyst\b", 5),
    (r"\bbusiness\s+analyst\b", 5),
    (r"\banalyst\b", 3),
    (r"\brev(?:enue)?\s*operations?\b", 20),
    (r"\bdata\s*operations?\b", 15),
    (r"\bquality\s*operations?\b", 15),
    (r"\brisk\s*operations?\b", 20),
    (r"\bmarketing\s*operations?\b", -15),
    (r"\bsales\s*operations?\b", -15),
    (r"\bgrowth\s*operations?\b", -20),
    (r"\boperations?\b", -2),
    (r"\bimplementation\s*specialist\b", 20),
    (r"\bimplementation\s*analyst\b", 20),
    (r"\bimplementation\s*engineer\b", -15),
    (r"\bimplementation\s*manager\b", -15),
    (r"\bimplementation\s*consultant\b", -15),
    # --- interaction type ---
    (r"\bclient[-\s]?facing\b", -5),
    (r"\bcustomer[-\s]?facing\b", -5),
    (r"\b(client|customer|vendor|supplier|merchant|partner)\s*onboarding\b", 20),
    (r"\bonboarding\s*(process|workflow|coordinator|program|platform|system)\b.{0,60}\b(client|customer|vendor|merchant|supplier)\b", 20),
    (r"\bonboard\s+(new\s+)?(clients?|customers?|vendors?|merchants?|suppliers?)\b", 20),
    (r"\bcustomer\s*support\b", -10),
    (r"\bcustomer\s*service\b", -10),
    (r"\bonboarding\s*(clients|customers|vendors|merchants|partners|users)\b", 10),
    (r"\bclient\s*onboarding\b", 10),
    (r"\bcustomer\s*onboarding\b", 10),
    (r"\bcustomer\s*success\s*(operations|enablement|ops?)\b", 8),
    (r"\bcustomer\s*success\s*enablement\b", 8),
    (r"\bsupport\s*operations\b", 8),
    (r"\bservice\s*operations\b", 8),
    (r"\boperational\s*support\b", 8),
    (r"\bcall\s*center\b", -20),
    (r"\binbound\s*call\b", -20),
    (r"\boutbound\s*call\b", -20),
    (r"\btechnical\s*support\b", 10),
    (r"\bapplication\s*support\b", 15),
    (r"\bproduction\s*support\b", 15),
    (r"\binternal\s*support\b", 15),
    # --- domain positives ---
    (r"\btrust\s*(&|and)\s*safety\b", 18),
    (r"\bcontent\s*moderation\b", 18),
    (r"\brisk\s*(operations|analyst|specialist|assessment)\b", 20),
    (r"\bcompliance\s*(analyst|officer|specialist|role|function|team|review|audit|monitoring|testing|assessment|reporting|program)\b", 20),
    (r"\binternal\s*audit\b", 20),
    (r"\baudit\s*(function|team|role|findings?|program|committee)\b", 20),
    (r"\bSOX\b", 20),
    (r"\bGDPR\b", 20),
    (r"\bFINRA\b", 20),
    (r"\bAML\b", 20),
    (r"\bKYC\b", 20),
    (r"\bBSA\b", 20),
    (r"\bregulatory\s*(compliance|reporting|requirement|framework|examination)\b", 20),
    (r"\bpolicy\s*(operations|enforcement|review)\b", 15),
    (r"\bdata\s*annotation\b", 18),
    (r"\bmodel\s*(evaluation|quality|ops)\b", 18),
    (r"\bRLHF\b", 18),
    (r"\bMLOps\b", 18),
    (r"\bdata\s*quality\b", 15),
    (r"\bdata\s*governance\b", 15),
    (r"\bdata\s*integrity\b", 15),
    # --- negative role signals ---
    (r"\bquota\b", -40),
    (r"\bquota[-\s]?carrying\b", -40),
    (r"\bOTE\b", -40),
    (r"\b(sales|revenue|deal|opportunity)\s*pipeline\b", -30),
    (r"\bpipeline\s+management\b.{0,40}\b(sales|revenue|crm|deals?)\b", -30),
    (r"\bbook\s*of\s*business\b", -50),
    (r"\bupsell\b", -40),
    (r"\b(customer|client|churn)\s*retention\b", -25),
    (r"\bretention\s*(rate|metric|goal|target|kpi)\b", -25),
    (r"\b(sales|customer|client)\s*engagement\b", -7),
    (r"\bengagement\s*(score|kpi|metric|rate)\b", -7),
    (r"\bevangelist\b", -25),
    (r"\bmanage\s*(a\s*)?(team|staff)\b", -12),
    (r"\b\d+\+?\s*direct\s*reports\b", -12),
    (r"\bpeople\s*manager\b", -12),
    (r"\btravel\s*(required|up\s*to|occasionally|frequently)\b", -10),
    (r"\b[1-9]\d?%\s*travel\b", -10),
    (r"\bon[-\s]?call\b", -6),
    (r"\bpager\s*duty\b", -6),
    (r"\bafter[-\s]?hours\s*support\b", -6),
    (r"\bor\s*equivalent\s*(experience|work\s*experience|job\s*experience)\b", 15),
    (r"\bequivalent\s*(experience|work\s*experience)\b", 15),
    (r"\b(bachelor'?s?|master'?s?|PhD)\s*(degree\s*)?(required|preferred)\b", -8),
    (r"\b(8|9|10|\d{2})\+?\s*years?\s*(of\s*)?experience\b", -4),
]

# ============================================================================
# SECTION 2D — Lifestyle lane config
# ============================================================================

LIFESTYLE_SIGNALS: List[LaneSignal] = [
    # positives
    (r"\bremote\b", 5),
    (r"\basync(hronous)?\b", 20),
    (r"\bself[-\s]?paced\b", 15),
    (r"\bflexible\s*(hours|schedule)\b", 10),
    (r"\bflextime\b", 10),
    (r"\blow[\s-]meeting\b", 15),
    (r"\bdeep\s*work\b", 12),
    (r"\bno[\s-]meeting\b", 12),
    (r"\bdocumentation[-\s]?driven\b", 8),
    (r"\bstrong\s*docs\b", 8),
    (r"\bdistributed\s*team\b", 8),
    (r"\bglobal\s*team\b", 8),
    (r"\bautonomous\b", 14),
    (r"\bindependent\s*contributor\b", 14),
    (r"\bself[-\s]?directed\b", 14),
    (r"\bsmall\s*team\b", 8),
    (r"\blean\s*team\b", 8),
    (r"\bstartup\b", 8),
    (r"\bevening\s*shift\b", 15),
    (r"\bnight\s*shift\b", 15),
    (r"\b(2nd|second|3rd|third)\s*shift\b", 15),
    (r"\bticket(ing)?\s*(system|queue|based|s)\b", 12),
    (r"\bSLA\b", 12),
    (r"\bqueue\b", 10),
    (r"\bdefined\s+(scope|deliverable|output|process|workflow)\b", 12),
    (r"\bstructured\s+(approach|process|workflow)\b", 12),
    (r"\bclear\s+(expectations?|deliverables?|scope)\b", 12),
    (r"\bprocess\s+improvement\b", 8),
    (r"\bworkflow\s*optim\w*\b", 8),
    (r"\bprocess\s+documentation\b", 8),
    (r"\bstandardiz\w+\s+(process|workflow|procedure)\b", 8),
    # negatives
    (r"\byou\s+will\s+own\b", -18),
    (r"\bown\s+the\s+\w*\s*(road\s*map|strategy|program|relationship|vision|outcomes?|product)\b", -18),
    (r"\blead\s+(the\s+)?(team|meeting|call|initiative|effort|workstream)\b", -15),
    (r"\bdrive\s+(adoption|change|alignment|strategy|outcomes?|transformation)\b", -12),
    (r"\bescalation\s+(point|owner)\b", -10),
    (r"\bact\s+as\s+(the\s+)?escalation\b", -10),
    (r"\bcoach(ing)?\s+(and\s+)?(mentor|develop)\b", -6),
    (r"\bmentor(ing)?\s+(and\s+)?(develop|coach)\b", -6),
    (r"\bgo.to\s+person\b", -20),
    (r"\bface\s+of\s+the\b", -20),
    (r"\bspokesperson\b", -20),
    (r"\b(primary|sole|main)\s+(point\s+of\s+contact|poc|liaison)\b", -10),
    (r"\bdynamic\s+environment\b", -8),
    (r"\bhigh[-\s]?pressure\b", -15),
    (r"\bwear\s+many\s+hats\b", -8),
    (r"\bscrappy\b", -8),
    (r"\bmove\s+fast\b", -8),
    (r"\bbias\s+(for|toward)\s+action\b", -8),
    (r"\brapidly\s+(changing|evolving)\b", -8),
    (r"\bstakeholder\s*management\b", -5),
    (r"\bcross[-\s]?functional\b", -2),
    (r"\bfast(?:[-\s]?(?:pace|paced))\b", -15),
    (r"\bfast[-\s]?moving\b", -10),
]

# ============================================================================
# SECTION 2E — Risk lane config
# ============================================================================

RISK_SIGNALS: List[LaneSignal] = [
    # ownership / accountability traps
    (r"\byou\s+will\s+own\b", 20),
    (r"\bown\s+the\s+\w*\s*(road\s*map|strategy|program|relationship|vision|outcomes?|product)\b", 18),
    (r"\blead\s+(the\s+)?(team|meeting|call|initiative|effort|workstream)\b", 15),
    (r"\bdrive\s+(adoption|change|alignment|strategy|outcomes?|transformation)\b", 12),
    (r"\bescalation\s+(point|owner)\b", 12),
    (r"\bact\s+as\s+(the\s+)?escalation\b", 12),
    (r"\b(primary|sole|main)\s+(point\s+of\s+contact|poc|liaison)\b", 10),
    (r"\bgo.to\s+person\b", 20),
    (r"\bface\s+of\s+the\b", 20),
    (r"\bspokesperson\b", 20),
    # stakeholder exposure
    (r"\bstakeholder\b", 8),
    (r"\bcross[-\s]?functional\b", 4),
    # sales / quota signals
    (r"\bquota\b", 30),
    (r"\bOTE\b", 25),
    (r"\bbook\s*of\s*business\b", 40),
    (r"\bupsell\b", 25),
    (r"\b(customer|client|churn)\s*retention\b", 15),
    (r"\b(sales|customer|client)\s*engagement\b", 8),
    # management
    (r"\b\d+\+?\s*direct\s*reports\b", 20),
    (r"\bpeople\s*manager\b", 20),
    (r"\bmanage\s*(a\s*)?(team|staff)\b", 15),
    # chaos culture
    (r"\bdynamic\s+environment\b", 10),
    (r"\bhigh[-\s]?pressure\b", 15),
    (r"\bwear\s+many\s+hats\b", 10),
    (r"\bscrappy\b", 8),
    (r"\bmove\s+fast\b", 10),
    (r"\bfast(?:[-\s]?(?:pace|paced))\b", 12),
    (r"\brapidly\s+(changing|evolving)\b", 8),
]

PRUNING_THRESHOLD_RULES: List[ThresholdRule] = [
    ("corporate_abstraction_penalty", 2, -7, [
        r"\bstrategic\s+initiatives\b",
        r"\bcross[-\s]functional\s+leadership\b",
        r"\bstakeholder\s+alignment\b",
        r"\benterprise[-\s]wide\b",
        r"\btransformational\b",
    ]),
    ("seniority_mismatch_penalty", 1, -5, [
        r"\b(3|5)\+?\s*years?\b",
        r"\bsubject\s*matter\s*expert\b",
        r"\bindependently\s*lead\s*initiatives\b",
    ]),
    ("hidden_customer_facing_penalty", 2, -8, [
        r"\bclient[-\s]facing\b",
        r"\bcustomer\s*interaction\b",
        r"\bpoint\s*of\s*contact\b",
        r"\bhandle\s*inquiries\b",
    ]),
    ("description_positive_bonus", 2, 5, [
        r"\bprocess\s*improvement\b",
        r"\bworkflow\s*optimization\b",
        r"\bautomation\b",
        r"\binternal\s*tools\b",
        r"\bdocumentation\b",
    ]),
    ("visibility_overload", 4, -15, [
        r"\byou\s+will\s+own\b",
        r"\bown\s+the\s+\w*\s*(road\s*map|strategy|program|vision|outcomes?|product)\b",
        r"\blead\s+(the\s+)?(team|meeting|initiative|effort|workstream)\b",
        r"\bdrive\s+(adoption|change|alignment|strategy|outcomes?)\b",
        r"\bescalation\s+(point|owner)\b",
        r"\bprimary\s+point\s+of\s+contact\b",
        r"\bgo.to\s+person\b",
        r"\bface\s+of\s+the\b",
    ]),
    ("urgency_chaos_culture", 3, -10, [
        r"\bdynamic\s+environment\b",
        r"\bhigh[-\s]?pressure\b",
        r"\bwear\s+many\s+hats\b",
        r"\bscrappy\b",
        r"\bmove\s+fast\b",
        r"\bbias\s+(for|toward)\s+action\b",
        r"\brapidly\s+(changing|evolving)\b",
        r"\bfast[-\s]?paced\b",
    ]),
    ("structured_ic_bonus", 3, 10, [
        r"\bprocess\s+improvement\b",
        r"\bworkflow\s*optim\b",
        r"\bticket(ing)?\s*(system|queue)\b",
        r"\bSLA\b",
        r"\bdefined\s+(scope|deliverable|output|process)\b",
        r"\bstructured\s+(approach|process|workflow)\b",
        r"\bdocumentation\b",
        r"\bautomation\b",
        r"\binternal\s+tools\b",
    ]),
]

MANAGER_LITE_PATTERNS = [
    r"\bmentor\b",
    r"\blead\s*initiatives\b",
    r"\bmanage\s*stakeholders\b",
    r"\bown\s*roadmap\b",
]

MANAGER_LITE_TITLE_BLOCKERS = [
    r"\bmanager\b",
    r"\blead\b",
    r"\bprincipal\b",
    r"\bdirector\b",
]

SCORING = {
    "base_title_score": 95,
    "title_penalty_stack_cap": 30,
    "title_alignment_mismatch_penalty": 10,
    "title_alignment_tags": [
        "operations", "analyst", "data", "implementation_onboarding",
        "it_support", "edi", "process", "quality_analyst", "risk_analyst", "compliance_analyst",
    ],
    "description_signal_scale": 0.75,
    "description_positive_cap": 20,
    "description_negative_cap": -20,
    "global_penalty_cap": -20,
    "manager_lite_penalty": -5,
    "duplicate_frequency_threshold": 2,
    "duplicate_frequency_penalty": 1,
    "require_pay": True,
    "apply_floor": 88,
    "manager_apply_floor": 82,
    "bad_apply_title_terms": ["engineer", "architect", "developer"],
    "customer_apply_prefixes": ["customer", "client", "account"],
}

# ============================================================================
# SECTION 2F — Confidence lane config
# ============================================================================

CONFIDENCE_SHORT_THRESHOLD = 40

CONTRADICTION_PAIRS: List[Tuple[str, str, int]] = [
    # (positive_signal_pattern, contradicting_negative_pattern, confidence_penalty)
    (r"\basync(hronous)?\b",         r"\bstakeholder\b",                          20),
    (r"\basync(hronous)?\b",         r"\bmeetings?\b",                             20),
    (r"\blow[\s-]meeting\b",         r"\blead\s+(the\s+)?(meeting|call)\b",        25),
    (r"\bno[\s-]meeting\b",          r"\bfacilitate\b",                            20),
    (r"\bself[-\s]?paced\b",         r"\bfast(?:[-\s]?(?:pace|paced))\b",          20),
    (r"\bindependent\s*contributor\b", r"\blead\s+(the\s+)?(team|initiative)\b",   20),
    (r"\bautonomous\b",              r"\bdrive\s+(adoption|change|alignment)\b",   15),
    (r"\bremote\b",                  r"\bon[-\s]?site\b",                          15),
]

# ============================================================================
# SECTION 2G — Archetype config
# ============================================================================

ARCHETYPES: List[Tuple[str, int, List[str]]] = [
    # (archetype_name, min_hits_to_classify, [patterns])
    ("stealth_sales", 2, [
        r"\bquota\b",
        r"\bOTE\b",
        r"\bbook\s*of\s*business\b",
        r"\bupsell\b",
        r"\bcold\s*call\b",
        r"\b(sales|revenue|deal)\s*pipeline\b",
        r"\baccount\s*(executive|manager)\b",
        r"\bbusiness\s*development\b",
    ]),
    ("fake_ops_support", 2, [
        r"\bcall\s*center\b",
        r"\binbound\s*calls?\b",
        r"\bcustomer\s*support\b",
        r"\bcustomer\s*service\b",
        r"\bhandle\s*inquiries\b",
        r"\bphone\s*support\b",
        r"\bticket\s*resolution\b",
    ]),
    ("corporate_theater", 3, [
        r"\bstrategic\s+initiatives\b",
        r"\bcross[-\s]?functional\s+leadership\b",
        r"\bstakeholder\s+alignment\b",
        r"\benterprise[-\s]?wide\b",
        r"\btransformational\b",
        r"\bdrive\s+(adoption|change|alignment|strategy)\b",
        r"\byou\s+will\s+own\b",
        r"\bgo.to\s+person\b",
    ]),
    ("safe_ic_ops", 3, [
        r"\bticket(ing)?\s*(system|queue|based)\b",
        r"\bSLA\b",
        r"\bdocumentation\b",
        r"\binternal\s*tools?\b",
        r"\bprocess\s+improvement\b",
        r"\bautomation\b",
        r"\basync(hronous)?\b",
        r"\bself[-\s]?paced\b",
        r"\blow[\s-]meeting\b",
    ]),
    ("chill_analyst", 2, [
        r"\banalyst\b",
        r"\bdata\s*(quality|governance|integrity)\b",
        r"\breporting\b",
        r"\bdashboard\b",
        r"\bprocess\s+improvement\b",
    ]),
]

# ============================================================================
# SECTION 2H — Decision thresholds
# ============================================================================

DECISION_THRESHOLDS: Dict[str, int] = {
    "risk_skip":          55,
    "confidence_review":  40,
    "lifestyle_review":   38,
    "apply_fit_min":      68,
    "apply_lifestyle_min": 58,
    "apply_comp_min":     45,
    "maybe_fit_min":      52,
}

# ============================================================================
# SECTION 3 — Internal helpers
# ============================================================================


def _norm(text: Optional[str]) -> str:
    if not text:
        return ""
    return re.sub(r"<[^>]+>", " ", text).lower()


def _match_any(text: str, patterns: List[str]) -> bool:
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns)


def _count_matches(text: str, patterns: List[str]) -> int:
    return sum(1 for pattern in patterns if re.search(pattern, text, re.IGNORECASE))


def _count_total_matches(text: str, patterns: List[str]) -> int:
    total = 0
    for pattern in patterns:
        total += len(re.findall(pattern, text, re.IGNORECASE))
    return total


def compute_visibility_risk(text: str) -> dict:
    text = (text or "").lower()
    hits = []
    for term in VISIBILITY_TERMS:
        if re.search(rf"\b{re.escape(term)}\b", text):
            hits.append(term)
    count = len(hits)
    if count <= VISIBILITY_THRESHOLDS["low"]:
        level = "low"
        penalty = 0
    elif count <= VISIBILITY_THRESHOLDS["medium"]:
        level = "medium"
        penalty = VISIBILITY_PENALTIES["medium"]
    else:
        level = "high"
        penalty = VISIBILITY_PENALTIES["high"]
    return {"count": count, "level": level, "penalty": penalty, "hits": hits}


def _hard_kill_check(title: str, description: str, company: str = "", url: str = "") -> Optional[str]:
    combined = f"{title} {description} {company} {url}".strip()
    for rule_name, patterns, title_only in HARD_KILLS:
        search_text = title if title_only else combined
        if _match_any(search_text, patterns):
            return rule_name
    for rule_name, threshold, patterns in HARD_KILL_THRESHOLD_RULES:
        if _count_total_matches(combined, patterns) >= threshold:
            return rule_name
    return None


def _apply_weighted_rules(text: str, rules) -> Tuple[int, List[str]]:
    score = 0
    tags: List[str] = []
    for tag, weight, patterns in rules:
        if _match_any(text, patterns):
            score += weight
            tags.append(tag)
    return score, tags


def _title_hint_score(title: str) -> Tuple[int, List[str]]:
    score = SCORING["base_title_score"]
    penalty_total = 0
    tags: List[str] = []

    for tag, patterns in TITLE_METADATA_TAGS:
        if _match_any(title, patterns):
            tags.append(tag)

    hint_score, hint_tags = _apply_weighted_rules(title, TITLE_HINTS)
    score += hint_score
    tags.extend(hint_tags)

    penalty_score, penalty_tags = _apply_weighted_rules(title, TITLE_PENALTIES)
    score += penalty_score
    penalty_total = -penalty_score if penalty_score < 0 else 0
    tags.extend(penalty_tags)

    alignment_tags = SCORING["title_alignment_tags"]
    alignment_count = sum(1 for tag in alignment_tags if tag in tags)
    if alignment_count == 0:
        score -= SCORING["title_alignment_mismatch_penalty"]
        penalty_total += SCORING["title_alignment_mismatch_penalty"]
        tags.append("alignment_mismatch_penalty")

    if penalty_total > SCORING["title_penalty_stack_cap"]:
        score += penalty_total - SCORING["title_penalty_stack_cap"]
        tags.append("penalty_stack_cap")

    for cap_tag, cap_value, patterns in TITLE_CAPS:
        if _match_any(title, patterns):
            score = min(score, cap_value)
            if cap_tag not in tags:
                tags.append(cap_tag)

    return _normalize_score(score), tags


def _pruning_adjustments(title: str, description: str) -> Tuple[int, List[str]]:
    combined = f"{title} {description}".strip()
    score = 0
    tags: List[str] = []
    for tag, threshold, weight, patterns in PRUNING_THRESHOLD_RULES:
        if _count_matches(combined, patterns) >= threshold:
            score += weight
            tags.append(tag)
    if _count_matches(combined, MANAGER_LITE_PATTERNS) >= 2 and not _match_any(title, MANAGER_LITE_TITLE_BLOCKERS):
        score += SCORING["manager_lite_penalty"]
        tags.append("manager_lite_penalty")
    return score, tags


def _normalize_score(raw: int) -> int:
    return max(0, min(100, raw))


def _tier_priority(tier: str) -> int:
    return {"SKIP": 0, "REVIEW": 1, "MAYBE": 2, "APPLY": 3}.get(tier, 0)


def _apply_tier_caps(tier: str, title: str, description: str, tags: List[str]) -> str:
    combined = f"{title} {description}".strip()
    if not combined:
        return tier
    capped_tier = tier
    for cap_name, max_tier, patterns in TIER_CAPS:
        if _match_any(combined, patterns) and _tier_priority(capped_tier) > _tier_priority(max_tier):
            capped_tier = max_tier
            tags.append(f"tier_cap:{cap_name}")
    return capped_tier


# ============================================================================
# SECTION 4 — Lane compute functions
# ============================================================================


def compute_fit(title: str, desc: str) -> int:
    """How well the role type aligns to target roles (0–100).
    Title is the primary signal; description role signals provide a capped adjustment.
    Pruning rules catch seniority mismatch and hidden customer-facing patterns."""
    title_score, _ = _title_hint_score(title)

    role_adj = 0
    for pattern, weight in FIT_ROLE_SIGNALS:
        if re.search(pattern, desc, re.IGNORECASE):
            role_adj += weight
    role_adj_scaled = max(-20, min(20, int(role_adj * 0.25)))

    pruning_adj, _ = _pruning_adjustments(title, desc)

    return _normalize_score(title_score + role_adj_scaled + pruning_adj)


def compute_lifestyle(desc: str) -> int:
    """Async/structured/low-visibility work culture fit (0–100, base 50)."""
    score = 50
    for pattern, weight in LIFESTYLE_SIGNALS:
        if re.search(pattern, desc, re.IGNORECASE):
            score += weight
    return max(0, min(100, score))


def compute_risk(title: str, desc: str) -> int:
    """Accountability traps, chaos culture, sales signals (0–100, higher = riskier)."""
    combined = f"{title} {desc}"
    score = 0
    for pattern, weight in RISK_SIGNALS:
        if re.search(pattern, combined, re.IGNORECASE):
            score += weight
    # Visibility term count compounds risk
    vis = compute_visibility_risk(combined)
    score += vis["count"] * 5
    return min(100, score)


def compute_comp(pay: str) -> int:
    """Pay quality signal (0–100). Returns 40 when pay is absent or unparseable."""
    low, unit = _extract_pay(pay)
    if low is None:
        return 40
    if unit == "year":
        if low >= 100_000:
            return 90
        if low >= 80_000:
            return 75
        if low >= 65_000:
            return 60
        if low >= 50_000:
            return 45
        return 10
    if unit == "hour":
        if low >= 45:
            return 90
        if low >= 35:
            return 75
        if low >= 28:
            return 60
        if low >= 25:
            return 45
        return 10
    return 40


def compute_confidence(desc: str, pay: str) -> int:
    """How reliable the job data is (0–100). Penalises short descriptions,
    missing pay, and internal contradictions (async but full of stakeholder-speak)."""
    score = 100
    word_count = len(desc.split()) if desc else 0
    if word_count < CONFIDENCE_SHORT_THRESHOLD:
        score -= 30
    elif word_count < 80:
        score -= 15
    if not (pay or "").strip():
        score -= 20
    for pos_pat, neg_pat, penalty in CONTRADICTION_PAIRS:
        if re.search(pos_pat, desc, re.IGNORECASE) and re.search(neg_pat, desc, re.IGNORECASE):
            score -= penalty
    return max(0, score)


def detect_archetype(title: str, desc: str) -> str:
    """Classify the job into a known archetype, or 'unknown'. First match wins."""
    combined = f"{title} {desc}"
    for archetype_name, min_hits, patterns in ARCHETYPES:
        hits = sum(1 for p in patterns if re.search(p, combined, re.IGNORECASE))
        if hits >= min_hits:
            return archetype_name
    return "unknown"


# ============================================================================
# SECTION 5 — Decision engine
# ============================================================================


def decide(
    fit: int,
    lifestyle: int,
    risk: int,
    comp: int,
    confidence: int,
    archetype: str,
    title: str,
    tags: List[str],
) -> str:
    """Convert lane scores into a tier using logic trees, not additive weights."""
    # Archetype hard overrides
    if archetype in ("stealth_sales", "fake_ops_support"):
        tags.append(f"archetype_override:{archetype}")
        return "SKIP"

    # Risk gate
    if risk > DECISION_THRESHOLDS["risk_skip"]:
        tags.append("risk_gate:skip")
        return "SKIP"

    # Confidence gate — uncertain data should not APPLY
    if confidence < DECISION_THRESHOLDS["confidence_review"]:
        tags.append("confidence_gate:review")
        return "REVIEW"

    # Lifestyle gate — wrong work culture is a dealbreaker
    if lifestyle < DECISION_THRESHOLDS["lifestyle_review"]:
        tags.append("lifestyle_gate:review")
        return "REVIEW"

    # Corporate theater cap — real fit doesn't matter if the job is BS
    if archetype == "corporate_theater":
        tags.append("archetype_cap:corporate_theater")
        return "REVIEW"

    # APPLY — all three key lanes must clear their thresholds
    if (
        fit >= DECISION_THRESHOLDS["apply_fit_min"]
        and lifestyle >= DECISION_THRESHOLDS["apply_lifestyle_min"]
        and comp >= DECISION_THRESHOLDS["apply_comp_min"]
    ):
        return _apply_tier_caps("APPLY", title, "", tags)

    # MAYBE — fit is solid but lifestyle or comp is borderline
    if fit >= DECISION_THRESHOLDS["maybe_fit_min"]:
        return _apply_tier_caps("MAYBE", title, "", tags)

    return "SKIP"


# ============================================================================
# SECTION 6 — Public API
# ============================================================================


def evaluate_job(job: dict) -> dict:
    """Multi-lane job evaluator. Replaces score_job().

    Returns lanes (fit, lifestyle, risk, comp, confidence), tier, archetype,
    tags, hard_kill, and visibility metadata. Also includes a derived 'score'
    field (= fit) for backward compatibility with existing callers."""
    title = _norm(job.get("title", ""))
    company = _norm(job.get("company", ""))
    url = _norm(job.get("url", ""))
    desc = _norm(job.get("description", "") or "")
    pay = str(job.get("pay", "") or "")

    # Hard kill runs before all lanes
    hard_kill = _hard_kill_check(title, desc, company, url)
    if hard_kill:
        return {
            "score": 0,
            "fit": 0,
            "lifestyle": 0,
            "risk": 100,
            "comp": 0,
            "confidence": 0,
            "tier": "SKIP",
            "tags": f"hard_kill:{hard_kill}",
            "hard_kill": hard_kill,
            "archetype": "killed",
            "visibility_count": 0,
            "visibility_level": "low",
            "visibility_hits": "",
            "scored_at": datetime.now(timezone.utc).isoformat(),
        }

    tags: List[str] = []

    fit        = compute_fit(title, desc)
    lifestyle  = compute_lifestyle(desc)
    risk       = compute_risk(title, desc)
    comp       = compute_comp(pay)
    confidence = compute_confidence(desc, pay)
    archetype  = detect_archetype(title, desc)

    tags.append(f"archetype:{archetype}")

    visibility = compute_visibility_risk(f"{title} {desc}")
    tags.append(f"visibility_{visibility['level']}")

    tier = decide(fit, lifestyle, risk, comp, confidence, archetype, title, tags)

    return {
        "score": fit,  # backward-compat alias for callers expecting a single integer
        "fit": fit,
        "lifestyle": lifestyle,
        "risk": risk,
        "comp": comp,
        "confidence": confidence,
        "tier": tier,
        "tags": "|".join(tags),
        "hard_kill": None,
        "archetype": archetype,
        "visibility_count": visibility["count"],
        "visibility_level": visibility["level"],
        "visibility_hits": "|".join(visibility["hits"]),
        "scored_at": datetime.now(timezone.utc).isoformat(),
    }
