"""
Standalone verification of the suspected Qase API behavior:
when POST /v1/run/{code} receives both `cases` AND `plan_id`, does the server
honor the explicit `cases` subset, or expand the run to the plan's full case set?

Creates a sandbox project in the target workspace from config.json, builds
a plan with 10 cases, creates 4 runs covering the relevant parameter combos,
reads back each run's actual case scope, prints a comparison table, then
deletes the sandbox project.

Safe: only touches the target workspace; uses a unique project code; cleans up.
Run from the repo root: python3 verify_plan_cases_bug.py
"""
import json
import sys
import time
import requests

CFG = json.load(open("config.json"))
TOKEN = CFG["target"]["api_token"]
HOST = CFG["target"].get("host", "qase.io")
SSL = CFG["target"].get("ssl", True)
SCHEME = "https" if SSL else "http"
DELIM = "." if HOST == "qase.io" else "-"
BASE = f"{SCHEME}://api{DELIM}{HOST}/v1"
HEADERS = {"Token": TOKEN, "Content-Type": "application/json", "accept": "application/json"}

PROJECT_CODE = f"PCBUG{int(time.time()) % 100000}"
PROJECT_TITLE = f"plan-cases bug verification {PROJECT_CODE}"


def call(method, path, **kw):
    url = f"{BASE}{path}"
    r = requests.request(method, url, headers=HEADERS, timeout=30, **kw)
    if r.status_code >= 400:
        print(f"  HTTP {r.status_code} {method} {path}: {r.text[:300]}")
        return None
    return r.json()


def get_run_case_count(run_id: int) -> int:
    j = call("GET", f"/run/{PROJECT_CODE}/{run_id}", params={"include": "cases"})
    if not j or not j.get("status"):
        return -1
    result = j.get("result") or {}
    cases = result.get("cases") or []
    return len(cases)


def main():
    print(f"BASE = {BASE}")
    print(f"sandbox project = {PROJECT_CODE}")

    print("\n[1] create sandbox project")
    res = call("POST", "/project", json={"title": PROJECT_TITLE, "code": PROJECT_CODE})
    if not res or not res.get("status"):
        print("FAILED to create project, aborting")
        sys.exit(1)

    try:
        print("\n[2] create 10 cases")
        case_ids = []
        for i in range(10):
            res = call("POST", f"/case/{PROJECT_CODE}", json={"title": f"case-{i+1}"})
            cid = (res or {}).get("result", {}).get("id")
            if not cid:
                print(f"  case {i+1} create failed"); sys.exit(1)
            case_ids.append(cid)
        print(f"  case_ids = {case_ids}")

        print("\n[3] create plan with all 10 cases")
        plan_res = call(
            "POST",
            f"/plan/{PROJECT_CODE}",
            json={"title": "plan-all-10", "cases": case_ids},
        )
        plan_id = (plan_res or {}).get("result", {}).get("id")
        if not plan_id:
            print("  plan create failed"); sys.exit(1)
        print(f"  plan_id = {plan_id}")

        subset = case_ids[:3]
        print(f"\n[4] subset for explicit cases = {subset}")

        scenarios = [
            ("A: cases only, no plan_id",        {"title": "run-A", "cases": subset}),
            ("B: cases + plan_id (bug repro?)",  {"title": "run-B", "cases": subset, "plan_id": plan_id}),
            ("C: plan_id only, no cases",        {"title": "run-C", "plan_id": plan_id}),
            ("D: empty cases [] + plan_id",      {"title": "run-D", "cases": [], "plan_id": plan_id}),
        ]

        rows = []
        created_run_for_patch = None
        for label, payload in scenarios:
            print(f"\n[5] create run — {label}")
            print(f"    payload = {payload}")
            res = call("POST", f"/run/{PROJECT_CODE}", json=payload)
            run_id = (res or {}).get("result", {}).get("id")
            if not run_id:
                rows.append((label, -1, -1, "CREATE FAILED"))
                continue
            n = get_run_case_count(run_id)
            sent = len(payload.get("cases") or [])
            note = ""
            if "plan_id" in payload and "cases" in payload and len(payload["cases"]) > 0:
                if n == sent:
                    note = "cases honored ✓"
                elif n == 10:
                    note = "EXPANDED to plan scope ✗ (bug)"
                else:
                    note = f"unexpected (n={n})"
            rows.append((label, sent, n, note))
            if label.startswith("A:") and run_id:
                created_run_for_patch = run_id

        # Now test post-create attempts to attach plan_id to an existing 3-case run
        # without expanding it.
        if created_run_for_patch:
            for label, method in [
                ("E: cases-only run, then PATCH plan_id", "PATCH"),
            ]:
                print(f"\n[6] {label}")
                res = call("POST", f"/run/{PROJECT_CODE}", json={"title": f"run-{label[0]}", "cases": subset})
                rid = (res or {}).get("result", {}).get("id")
                if not rid:
                    rows.append((label, len(subset), -1, "CREATE FAILED"))
                    continue
                before = get_run_case_count(rid)
                upd = call(method, f"/run/{PROJECT_CODE}/{rid}", json={"plan_id": plan_id})
                after = get_run_case_count(rid)
                # Read the run back fully and inspect plan association
                detail = call("GET", f"/run/{PROJECT_CODE}/{rid}", params={"include": "cases"})
                result = (detail or {}).get("result") or {}
                attached_plan = (
                    result.get("plan_id")
                    or (result.get("plan") or {}).get("id") if isinstance(result.get("plan"), dict) else None
                )
                plan_attached = attached_plan == plan_id
                ok_upd = bool(upd and upd.get("status"))
                if after == before == len(subset) and plan_attached:
                    note = "cases preserved + plan attached ✓✓"
                elif after == before == len(subset):
                    note = f"cases preserved ✓, BUT plan NOT attached (plan_id in run: {attached_plan!r})"
                elif after == 10:
                    note = "EXPANDED after update ✗"
                else:
                    note = f"unexpected (before={before}, after={after})"
                if not ok_upd:
                    note += " [update HTTP failed]"
                rows.append((label, len(subset), after, note))

        print("\n=========== RESULT ===========")
        print(f"{'scenario':<40} {'sent':>5} {'actual':>7}  note")
        print("-" * 85)
        for label, sent, n, note in rows:
            print(f"{label:<40} {sent:>5} {n:>7}  {note}")
        print()
        print("Interpretation:")
        print("  - If B shows actual=10, the bug is confirmed: plan_id auto-expands the run scope.")
        print("  - If B shows actual=3, then cases is honored and the bug is elsewhere.")

    finally:
        print(f"\n[cleanup] deleting sandbox project {PROJECT_CODE}")
        call("DELETE", f"/project/{PROJECT_CODE}")


if __name__ == "__main__":
    main()
