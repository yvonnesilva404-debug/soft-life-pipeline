"""Read Supabase export CSV, apply detection, write jobs.json for frontend."""
import csv, json, sys
from pathlib import Path
sys.path.insert(0, '.')
import pipeline

CSV = r'C:\Users\User\Downloads\softlifecreed-jobs-2026-06-09.csv'
JSON_OUT = r'C:\Users\User\Documents\work\Soft_Creed\jobs.json'

with open(CSV, encoding='utf-8') as f:
    rows = list(csv.DictReader(f))

seen_urls = set()
output = []
for row in rows:
    url = (row.get('url') or '').strip()
    if url and url in seen_urls:
        continue
    if url:
        seen_urls.add(url)

    title = (row.get('title') or '').strip()
    location = (row.get('location') or '').strip()
    tier = (row.get('tier') or '').strip()
    tier_rank = int(row.get('tier_rank') or 4)
    tier_icon = row.get('tier_icon') or ''
    pay = (row.get('pay') or '').strip() or 'TBD'
    exp_level = (row.get('exp_level') or '').strip()
    category = (row.get('category') or '').strip()
    date_label = (row.get('date_label') or '').strip()
    is_new = (row.get('is_new') or '').strip().lower() == 'true'
    has_live = (row.get('has_live_url') or '').strip().lower() == 'true'

    # Detection
    pt = pipeline._is_part_time_job(title)
    nt = pipeline._is_night_time_job(title)
    nu = pipeline._is_non_us_location(location)

    output.append({
        "date": date_label,
        "date_label": date_label,
        "tier": tier,
        "tier_rank": tier_rank,
        "tier_icon": tier_icon,
        "pay": pay,
        "title": title,
        "exp_level": exp_level,
        "location": location,
        "category": category,
        "url": url,
        "has_live_url": has_live,
        "is_new": is_new,
        "part_time": pt,
        "night_time": nt,
        "non_us": nu,
    })

# Sort by date desc, then tier rank
output.sort(key=lambda r: (str(r.get('date', '')), int(r.get('tier_rank', 4))), reverse=True)

Path(JSON_OUT).parent.mkdir(parents=True, exist_ok=True)
with open(JSON_OUT, 'w', encoding='utf-8') as f:
    json.dump(output, f, indent=2)

print(f"Wrote {len(output)} jobs to {JSON_OUT}")
pt_count = sum(1 for r in output if r['part_time'])
nt_count = sum(1 for r in output if r['night_time'])
nu_count = sum(1 for r in output if r['non_us'])
print(f"Labels: Part Time={pt_count}, Night Time={nt_count}, Non-US={nu_count}")
