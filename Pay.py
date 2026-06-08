import re

# ---------------------------------------------------------------------------
# Patterns — hourly first, annual second, generic range fallback
# ---------------------------------------------------------------------------

_DASH = r"[-\u2013\u2014]"
_CURRENCY = r"(?:USD|CAD|AUD|GBP|EUR|NZD|SGD|HKD|JPY|CNY)"

_NUM = r"\$?([\d,]+(?:\.\d+)?)([kK])?\s*(?:" + _CURRENCY + r")?\b\.?"
_RANGE = _NUM + r"\s*(?:" + _DASH + r"|to|and)\s*" + _NUM

_HR = re.compile(
    _NUM
    + r"\s*(?:" + _DASH + r"|to|and)\s*"
    + _NUM
    + r"[^$\n]{0,80}?\b(?:/\s*hr|per\s*hour|hourly)\b",
    re.IGNORECASE,
)

_YR = re.compile(
    _NUM
    + r"\s*(?:" + _DASH + r"|to)\s*"
    + _NUM
    + r"[^$\n]{0,80}?\b(?:/\s*yr|/\s*year|per\s*year|yearly|annual(?:ly)?|salary|base\s+salary|base\s+pay)\b",
    re.IGNORECASE,
)

_GEN = re.compile(
    _RANGE,
    re.IGNORECASE,
)

_YEAR_SINGLE = re.compile(
    _NUM
    + r"[^$\n]{0,80}?\b(?:/\s*yr|/\s*year|per\s*year|yearly|annual(?:ly)?|salary|base\s+salary|base\s+pay)\b",
    re.IGNORECASE,
)

_HR_SINGLE = re.compile(
    _NUM
    + r"[^$\n]{0,80}?\b(?:/\s*hr|per\s*hour|hourly)\b",
    re.IGNORECASE,
)

_PAY_SINGLE = re.compile(
    _NUM,
    re.IGNORECASE,
)

_YEAR_HINTS = re.compile(
    r"\b(?:annual|year|salary|base\s+salary|base\s+pay|local(?:\s+currency)?)\b",
    re.IGNORECASE,
)

_PAY_HINTS = re.compile(
    r"\b(?:salary|compensation|pay|wage|annual|yearly|per\s*year|/yr|USD|CAD|AUD|GBP|EUR|NZD|SGD|HKD|JPY|CNY)\b",
    re.IGNORECASE,
)

_DIRECT_PAY_HINTS = re.compile(
    r"\b(?:salary|compensation|pay|wage|rate|pay\s*range|base\s+salary|base\s+pay)\b",
    re.IGNORECASE,
)

_BENEFIT_CONTEXT = re.compile(
    r"\b(?:401\s*\(?k\)?|403\s*\(?b\)?|retirement|benefit|benefits|matching|match|contribution|contributions)\b",
    re.IGNORECASE,
)

_NON_PAY_CONTEXT = re.compile(
    r"\b(?:stipend|allowance|reimbursement|professional\s+development|learning\s+budget|training\s+budget|"
    r"education\s+budget|equipment\s+budget|wellness\s+budget|home\s+office\s+budget|perks?)\b",
    re.IGNORECASE,
)

_SUSPICIOUS_NUMERIC_PAY = re.compile(r"^\s*(?:\d{1,2}[-\u2013\u2014]\d{1,2}|\d{3,4}[-\u2013\u2014]\d{3,4})\s*$")


def _n(s: str, k_group: str = None) -> float:
    cleaned = s.replace(',', '')
    if not cleaned:
        return 0.0
    return float(cleaned) * (1000 if k_group else 1)


def _currency_from_match(text: str) -> str:
    m = re.search(r"\b(USD|CAD|AUD|GBP|EUR|NZD|SGD|HKD|JPY|CNY)\b", text, re.IGNORECASE)
    return m.group(1).upper() if m else ""


def _is_annual_context(text: str, start: int, end: int) -> bool:
    window = text[max(0, start - 80): min(len(text), end + 80)]
    return bool(_YEAR_HINTS.search(window))


def _format_range(lo: float, hi: float, suffix: str) -> str:
    return f"${lo:,.0f} \u2013 ${hi:,.0f}{suffix}"


MAX_YEARLY_PAY = 1_000_000
MAX_HOURLY_PAY = 1_000


def _parse_range_match(match):
    groups = match.groups()
    lo = _n(groups[0], groups[1])
    hi = _n(groups[2], groups[3])
    currency = _currency_from_match(match.group(0))
    return lo, hi, currency


def _is_plausible_range(lo: float, hi: float, suffix: str) -> bool:
    if hi is None:
        return lo > 0 and lo <= MAX_YEARLY_PAY
    if lo <= 0 or hi <= 0 or hi < lo:
        return False
    normalized = suffix.lower()
    if any(tok in normalized for tok in ("per year", "per yr", "/yr", "/year", "annual", "annually", "yearly", "salary", "base salary", "base pay")):
        if hi < 15000:
            return False
        return lo <= MAX_YEARLY_PAY and hi <= MAX_YEARLY_PAY
    if any(tok in normalized for tok in ("/hr", "hour", "hourly")):
        return lo <= MAX_HOURLY_PAY and hi <= MAX_HOURLY_PAY
    return True


def _is_valid_range(lo: float, hi: float) -> bool:
    return lo > 0 and hi > 0 and hi >= lo


def _parse_single_match(match):
    groups = match.groups()
    lo = _n(groups[0], groups[1])
    currency = _currency_from_match(match.group(0))
    return lo, currency


def _is_match_in_range_context(text: str, match) -> bool:
    start, end = match.span()
    window = text[max(0, start - 10): min(len(text), end + 10)]
    return bool(re.search(r"[-]|\b(to|and)\b", window, re.IGNORECASE))


def _has_money_marker(text: str) -> bool:
    return bool(re.search(r"\$|\b(?:USD|CAD|AUD|GBP|EUR|NZD|SGD|HKD|JPY|CNY)\b", text, re.IGNORECASE))


def _has_non_pay_context(text: str, match, radius: int = 80) -> bool:
    start, end = match.span()
    window = text[max(0, start - radius): min(len(text), end + radius)]
    return bool(_BENEFIT_CONTEXT.search(window) or _NON_PAY_CONTEXT.search(window))


def extract_pay(text: str) -> str:
    """
    Scan *text* for the best credible pay signal.
    """
    if not text:
        return ""
    text = re.sub(r"\b(?:401\s*\(?k\)?|403\s*\(?b\)?)\b", "", text, flags=re.IGNORECASE)
    if _SUSPICIOUS_NUMERIC_PAY.match(text) and not _PAY_HINTS.search(text):
        return ""
    candidates = []

    for match in _YR.finditer(text):
        if _has_non_pay_context(text, match):
            continue
        lo, hi, currency = _parse_range_match(match)
        if _is_plausible_range(lo, hi, match.group(0)):
            candidates.append(("year_range", lo, hi, currency, match.start(), match))

    for match in _GEN.finditer(text):
        if _has_non_pay_context(text, match):
            continue
        lo, hi, currency = _parse_range_match(match)
        if not _is_valid_range(lo, hi):
            continue
        if _is_annual_context(text, match.start(), match.end()) or hi >= 10000:
            if _is_plausible_range(lo, hi, "annual"):
                candidates.append(("generic_year_range", lo, hi, currency, match.start(), match))
        elif currency:
            if _is_plausible_range(lo, hi, "hourly"):
                candidates.append(("generic_hour_range", lo, hi, currency, match.start(), match))

    for match in _HR.finditer(text):
        if _has_non_pay_context(text, match):
            continue
        lo, hi, currency = _parse_range_match(match)
        if lo <= MAX_HOURLY_PAY and (hi is None or hi <= MAX_HOURLY_PAY):
            candidates.append(("hour_range", lo, hi, currency, match.start(), match))

    for match in _YEAR_SINGLE.finditer(text):
        if _has_non_pay_context(text, match):
            continue
        lo, currency = _parse_single_match(match)
        if lo < 15000:
            continue
        if lo <= MAX_YEARLY_PAY:
            candidates.append(("year_single", lo, None, currency, match.start(), match))

    for match in _PAY_SINGLE.finditer(text):
        lo, currency = _parse_single_match(match)
        window = text[max(0, match.start() - 30): min(len(text), match.end() + 30)]
        if _BENEFIT_CONTEXT.search(window):
            continue
        if lo < 10000:
            if (
                lo <= MAX_HOURLY_PAY
                and _DIRECT_PAY_HINTS.search(window)
                and (_has_money_marker(match.group(0)) or re.search(r"\b(?:/\s*hr|per\s*hour|hourly)\b", window, re.IGNORECASE))
            ):
                candidates.append(("generic_hour_single", lo, None, currency, match.start(), match))
        else:
            if _PAY_HINTS.search(window) and lo <= MAX_YEARLY_PAY:
                candidates.append(("generic_year_single", lo, None, currency, match.start(), match))

    for match in _HR_SINGLE.finditer(text):
        if _has_non_pay_context(text, match):
            continue
        lo, currency = _parse_single_match(match)
        if lo <= MAX_HOURLY_PAY:
            candidates.append(("hour_single", lo, None, currency, match.start(), match))

    if not candidates:
        return ""

    rank = {
        "year_range": 5,
        "generic_year_range": 4,
        "year_single": 3,
        "generic_year_single": 3,
        "generic_hour_range": 2,
        "hour_range": 2,
        "hour_single": 1,
        "generic_hour_single": 1,
        "generic_range": 0,
    }

    candidates.sort(key=lambda c: (
        0 if c[3] == "USD" else 1,
        -rank.get(c[0], 0),
        -c[1],
        c[4],
    ))

    kind, lo, hi, currency, _, match = candidates[0]
    if hi is not None:
        return _format_range(lo, hi, "/yr" if kind in ("year_range", "generic_year_range") else "/hr")
    if kind in ("year_single", "generic_year_single"):
        return f"${lo:,.0f}/yr"
    if kind in ("hour_single", "generic_hour_single"):
        return f"${lo:,.0f}/hr"
    return ""
