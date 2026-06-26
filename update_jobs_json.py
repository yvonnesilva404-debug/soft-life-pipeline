"""Fetch jobs from Supabase REST API and write jobs.json for frontend."""
import json, os
from pathlib import Path
import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
JSON_OUT = r'C:\Users\User\Documents\work\Soft_Creed\jobs.json'

headers = {
    "apikey": SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type": "application/json",
}

url = f"{SUPABASE_URL.rstrip('/')}/rest/v1/jobs?select=id,date_label,tier,tier_rank,tier_icon,pay,title,exp_level,location,url,has_live_url,is_new,category,part_time,night_time,non_us&order=id.asc"
r = requests.get(url, headers=headers)
r.raise_for_status()
rows: list = r.json()

seen_urls = set()
output = []
for row in rows:
    url = (row.get('url') or '').strip()
    if url and url in seen_urls:
        continue
    if url:
        seen_urls.add(url)

    output.append({
        "date": row.get('date_label', ''),
        "date_label": row.get('date_label', ''),
        "tier": row.get('tier', ''),
        "tier_rank": int(row.get('tier_rank', 4) or 4),
        "tier_icon": row.get('tier_icon', ''),
        "pay": (row.get('pay') or '').strip() or 'TBD',
        "title": row.get('title', ''),
        "exp_level": row.get('exp_level', ''),
        "location": row.get('location', ''),
        "category": row.get('category', ''),
        "url": url,
        "has_live_url": str(row.get('has_live_url', '')).lower() == 'true',
        "is_new": bool(row.get('is_new', False)),
        "part_time": bool(row.get('part_time', False)),
        "night_time": bool(row.get('night_time', False)),
        "non_us": bool(row.get('non_us', False)),
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
