"""
Post-migration verification: do migrated runs have the same case scope as the
source? Matches source → target via mappings.json (preserve_ids does not always
hold), so each comparison is between the actual paired runs.

Special attention to runs whose source had a plan_id — those are the ones
exposed to the plan-cases bug.
"""
import json
import sys
import time
from collections import defaultdict
import requests

CFG = json.load(open("config.json"))
SRC = ("https://api.qase.io/v1", CFG["source"]["api_token"])
TGT = ("https://api.qase.io/v1", CFG["target"]["api_token"])
MAPPINGS = json.load(open("mappings.json"))
PROJECTS = ["ECSE", "DAT"]


def get_run(base, token, code, rid, retries=6):
    delay = 1.0
    for attempt in range(retries):
        try:
            r = requests.get(
                f"{base}/run/{code}/{rid}",
                headers={"Token": token, "accept": "application/json"},
                params={"include": "cases"},
                timeout=60,
            )
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
            time.sleep(delay); delay = min(delay * 2, 20); continue
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(delay); delay = min(delay * 2, 20); continue
        if r.status_code == 404:
            return None
        if r.status_code != 200:
            return None
        return (r.json() or {}).get("result") or None
    return None


def info(run):
    if not run:
        return (-1, None, None, None)
    cases = run.get("cases") or []
    plan = run.get("plan_id")
    if plan is None and isinstance(run.get("plan"), dict):
        plan = run["plan"].get("id")
    return (len(cases), plan, run.get("title", "")[:60], run.get("status"))


def main():
    grand = defaultdict(int)
    sample_mismatches = []
    sample_plan_runs = []
    for code in PROJECTS:
        print(f"\n=== {code} ===")
        run_map = MAPPINGS.get("runs", {}).get(code, {}) or {}
        print(f"  source→target run mappings: {len(run_map)}")
        for src_id_str, tgt_id in sorted(run_map.items(), key=lambda kv: int(kv[0])):
            src_id = int(src_id_str)
            src = get_run(*SRC, code, src_id)
            tgt = get_run(*TGT, code, int(tgt_id))
            s_n, s_plan, s_title, _ = info(src)
            t_n, t_plan, t_title, _ = info(tgt)
            grand["compared"] += 1
            if s_n == t_n:
                grand["match"] += 1
            else:
                grand["mismatch"] += 1
                sample_mismatches.append({
                    "code": code, "src_id": src_id, "tgt_id": tgt_id,
                    "src_n": s_n, "tgt_n": t_n,
                    "src_plan": s_plan, "title": s_title,
                })
            if s_plan:
                grand["plan_runs"] += 1
                if s_n == t_n:
                    grand["plan_runs_match"] += 1
                sample_plan_runs.append({
                    "code": code, "src_id": src_id, "tgt_id": tgt_id,
                    "src_n": s_n, "tgt_n": t_n,
                    "src_plan": s_plan, "tgt_plan": t_plan,
                    "title": s_title, "match": s_n == t_n,
                })
            time.sleep(0.15)

    print("\n=========== SUMMARY ===========")
    print(f"runs compared: {grand['compared']}")
    print(f"matching case counts: {grand['match']}")
    print(f"mismatches: {grand['mismatch']}")
    print(f"plan-based source runs: {grand['plan_runs']}")
    print(f"  of which match: {grand['plan_runs_match']}")
    print()
    if grand["mismatch"] == 0:
        print("FIX VERIFIED: every migrated run has the same case scope as its source.")
    else:
        print(f"MISMATCHES (showing up to 15):")
        for s in sample_mismatches[:15]:
            print(f"  {s['code']} src#{s['src_id']:>3} (plan={s['src_plan']}) → "
                  f"tgt#{s['tgt_id']:>3}: src={s['src_n']:>3} tgt={s['tgt_n']:>3}  {s['title']!r}")
    print()
    if sample_plan_runs:
        print(f"All plan-based source runs ({len(sample_plan_runs)}):")
        for s in sample_plan_runs:
            mark = "✓" if s["match"] else "✗"
            print(f"  {mark} {s['code']} src#{s['src_id']:>3} (plan={s['src_plan']}) → "
                  f"tgt#{s['tgt_id']:>3}: src={s['src_n']:>3} tgt={s['tgt_n']:>3} tgt_plan={s['tgt_plan']}  "
                  f"{s['title']!r}")


if __name__ == "__main__":
    main()
