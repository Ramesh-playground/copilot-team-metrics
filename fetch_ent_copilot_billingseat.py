import os
import csv
import time
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import re

load_dotenv()

API_BASE = os.getenv("API_BASE") or "https://api.github.com"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
ENTERPRISE_SLUG = os.getenv("ENTERPRISE_SLUG")
OUTPUT_CSV = os.getenv("OUTPUT_CSV") or f"enterprise_teams_users_copilot_{datetime.now().strftime('%Y%m%d')}.csv"

# Optional override if your suffix is not derived correctly from enterprise slug
# Example: LOGIN_SUFFIX=newgen  -> schander_newgen
LOGIN_SUFFIX = (os.getenv("LOGIN_SUFFIX") or "").strip().lower()

if not GITHUB_TOKEN:
    raise SystemExit("Missing GITHUB_TOKEN in environment (.env).")
if not ENTERPRISE_SLUG:
    raise SystemExit("Missing ENTERPRISE_SLUG in environment (.env).")

HEADERS_JSON = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
HEADERS_SCIM = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/scim+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

SESSION = requests.Session()


def gh_get(url, headers, params=None, timeout=60):
    last = None
    for attempt in range(1, 7):
        resp = SESSION.get(url, headers=headers, params=params, timeout=timeout)
        last = resp

        if resp.status_code in (403, 429, 500, 502, 503, 504):
            retry_after = resp.headers.get("Retry-After")
            wait = int(retry_after) if retry_after and retry_after.isdigit() else min(30, 2 * attempt)
            time.sleep(wait)
            continue

        return resp
    return last


def normalize_list_payload(payload, keys):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in keys:
            v = payload.get(k)
            if isinstance(v, list):
                return v
    raise RuntimeError(f"Unsupported list payload shape: {type(payload)}")


def fetch_rest_list_paged(url, headers, keys, per_page=100, extra_params=None):
    out = []
    page = 1
    while True:
        params = dict(extra_params or {})
        params["per_page"] = per_page
        params["page"] = page

        resp = gh_get(url, headers=headers, params=params)
        if resp.status_code in (403, 404):
            raise requests.HTTPError(f"{resp.status_code} for {url}: {resp.text}", response=resp)

        resp.raise_for_status()
        items = normalize_list_payload(resp.json(), keys=keys)
        out.extend(items)

        if len(items) < per_page:
            break
        page += 1
    return out


# -------------------------
# SCIM
# -------------------------
def fetch_all_scim_users():
    url = f"{API_BASE}/scim/v2/enterprises/{ENTERPRISE_SLUG}/Users"

    start_index = 1
    count = 100
    users = []

    while True:
        resp = gh_get(url, headers=HEADERS_SCIM, params={"startIndex": start_index, "count": count})
        resp.raise_for_status()

        payload = resp.json() or {}
        resources = payload.get("Resources") or []
        users.extend(resources)

        total_results = int(payload.get("totalResults") or 0)
        items_per_page = int(payload.get("itemsPerPage") or len(resources) or 0)

        if items_per_page <= 0:
            break

        start_index += items_per_page
        if start_index > total_results:
            break

    return users


def pick_scim_email(u: dict) -> str:
    emails = u.get("emails") or []
    if isinstance(emails, list) and emails:
        primary = next(
            (e for e in emails if isinstance(e, dict) and e.get("primary") is True and e.get("value")),
            None,
        )
        if primary:
            return str(primary.get("value") or "").strip()

        first = next((e for e in emails if isinstance(e, dict) and e.get("value")), None)
        if first:
            return str(first.get("value") or "").strip()

    user_name = str(u.get("userName") or "").strip()
    return user_name if "@" in user_name else ""


def pick_scim_name(u: dict) -> str:
    dn = str(u.get("displayName") or "").strip()
    if dn:
        return dn

    name_obj = u.get("name") or {}
    if isinstance(name_obj, dict):
        formatted = str(name_obj.get("formatted") or "").strip()
        if formatted:
            return formatted
        given = str(name_obj.get("givenName") or "").strip()
        family = str(name_obj.get("familyName") or "").strip()
        full = " ".join([p for p in [given, family] if p]).strip()
        if full:
            return full

    return ""


def derive_suffix_token() -> str:
    """
    For ENTERPRISE_SLUG like 'Newgen-EMU', default suffix becomes 'newgen'
    Override with env LOGIN_SUFFIX if needed.
    """
    if LOGIN_SUFFIX:
        return LOGIN_SUFFIX
    # take first token before '-' as a reasonable default
    return (ENTERPRISE_SLUG.split("-", 1)[0] or "").strip().lower()


def generate_login_candidates_from_email(email: str) -> set[str]:
    """
    Generate likely GitHub EMU logins from SCIM email.
    Example:
      s.chander@domain.com -> schander_newgen (dot removed) or s-chander_newgen (dot->hyphen)
      g-singh@domain.com -> g-singh_newgen
    """
    out = set()
    email = (email or "").strip().lower()
    if "@" not in email:
        return out

    local = email.split("@", 1)[0].strip()
    if not local:
        return out

    suffix = derive_suffix_token()

    # Variants of local-part:
    variants = set()
    variants.add(local)                      # keep as-is
    variants.add(local.replace(".", ""))     # remove dots
    variants.add(local.replace(".", "-"))    # dots to hyphen
    variants.add(local.replace("_", "-"))    # underscores to hyphen
    variants.add(re.sub(r"[^a-z0-9\-]", "", local))  # keep hyphen, strip others
    variants.add(re.sub(r"[^a-z0-9]", "", local))    # strict alnum only

    # Add suffix variants (common EMU: <base>_<suffix>)
    for v in list(variants):
        v = v.strip("-").strip()
        if not v:
            continue
        out.add(v)
        if suffix:
            out.add(f"{v}_{suffix}")

    return out


def build_scim_index(scim_users):
    """
    Build an index that can match TEAM login -> SCIM user.

    Index keys include:
      - full email
      - email local-part
      - generated EMU login candidates (like schander_newgen)
      - SCIM userName (sometimes equals email)
    """
    idx = {}
    for u in scim_users:
        if not isinstance(u, dict):
            continue

        name = pick_scim_name(u)
        email = pick_scim_email(u)
        scim_user_name = str(u.get("userName") or "").strip()

        keys = set()

        if email:
            keys.add(email.lower())
            keys.add(email.split("@", 1)[0].lower())
            keys |= generate_login_candidates_from_email(email)

        if scim_user_name:
            keys.add(scim_user_name.lower())
            if "@" in scim_user_name:
                keys.add(scim_user_name.split("@", 1)[0].lower())
                keys |= generate_login_candidates_from_email(scim_user_name)

        for k in keys:
            if not k:
                continue
            idx.setdefault(
                k,
                {"name": name, "email": email, "scim_userName": scim_user_name},
            )

    return idx


# -------------------------
# Copilot seats
# -------------------------
def fetch_copilot_billing_seats_by_login():
    url = f"{API_BASE}/enterprises/{ENTERPRISE_SLUG}/copilot/billing/seats"

    all_seats = []
    page = 1
    per_page = 100
    while True:
        resp = gh_get(url, headers=HEADERS_JSON, params={"per_page": per_page, "page": page})
        resp.raise_for_status()
        payload = resp.json() or {}
        seats = payload.get("seats", []) or []
        all_seats.extend(seats)
        if len(seats) < per_page:
            break
        page += 1

    by_login = {}
    for s in all_seats:
        login = ((s.get("assignee") or {}).get("login") or "").strip()
        if login:
            by_login[login] = s
    return by_login


def is_active(last_activity_at):
    if not last_activity_at:
        return "inactive"
    try:
        last_activity = datetime.fromisoformat(last_activity_at.replace("Z", "+00:00"))
        now = datetime.now(last_activity.tzinfo)
        return "active" if (now - last_activity) <= timedelta(days=30) else "inactive"
    except Exception:
        return "inactive"


# -------------------------
# Enterprise teams & memberships
# -------------------------
def fetch_enterprise_teams():
    url = f"{API_BASE}/enterprises/{ENTERPRISE_SLUG}/teams"
    return fetch_rest_list_paged(url, headers=HEADERS_JSON, keys=("teams", "items", "data"), per_page=100)


def fetch_enterprise_team_memberships(team_slug):
    url = f"{API_BASE}/enterprises/{ENTERPRISE_SLUG}/teams/{team_slug}/memberships"
    return fetch_rest_list_paged(url, headers=HEADERS_JSON, keys=("memberships", "items", "data"), per_page=100)


def parse_membership_login(m):
    if not isinstance(m, dict):
        return ""
    for path in (("user", "login"), ("member", "login"), ("login",)):
        cur = m
        ok = True
        for p in path:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                ok = False
                break
        if ok and isinstance(cur, str) and cur.strip():
            return cur.strip()
    return ""


def main():
    print(f"Enterprise: {ENTERPRISE_SLUG}")
    print(f"Derived login suffix token: {derive_suffix_token()} (override with LOGIN_SUFFIX env if needed)")

    print("Fetching SCIM users...")
    scim_users = fetch_all_scim_users()
    scim_index = build_scim_index(scim_users)
    print(f"SCIM users fetched: {len(scim_users)}; SCIM index keys: {len(scim_index)}")

    print("Fetching Copilot seats...")
    seats_by_login = fetch_copilot_billing_seats_by_login()
    print(f"Copilot seats indexed by login: {len(seats_by_login)}")

    print("Fetching enterprise teams...")
    teams = fetch_enterprise_teams()
    print(f"Enterprise teams fetched: {len(teams)}")

    rows = []
    no_scim_match = 0

    for i, t in enumerate(teams, start=1):
        team_name = (t.get("name") or t.get("display_name") or t.get("slug") or "").strip()
        team_slug = (t.get("slug") or t.get("team_slug") or "").strip()
        if not team_slug:
            continue

        print(f"[{i}/{len(teams)}] Fetching users for team: {team_name} ({team_slug})")
        memberships = fetch_enterprise_team_memberships(team_slug)

        for m in memberships:
            login = parse_membership_login(m)
            if not login:
                continue

            key = login.lower().strip()
            scim = scim_index.get(key) or {}

            if not scim:
                no_scim_match += 1

            seat = seats_by_login.get(login)

            rows.append(
                {
                    "enterprise": ENTERPRISE_SLUG,
                    "team_name": team_name,
                    "team_slug": team_slug,
                    "login": login,
                    "name": scim.get("name", ""),
                    "email": scim.get("email", ""),
                    "scim_userName": scim.get("scim_userName", ""),
                    "copilot_assigned": "yes" if seat else "no",
                    "copilot_status": (seat or {}).get("status", "") if seat else "",
                    "plan_type": (seat or {}).get("plan_type", "") if seat else "",
                    "last_activity_at": (seat or {}).get("last_activity_at", "") if seat else "",
                    "active_status": is_active((seat or {}).get("last_activity_at")) if seat else "inactive",
                    "seat_created_at": (seat or {}).get("created_at", "") if seat else "",
                    "seat_updated_at": (seat or {}).get("updated_at", "") if seat else "",
                }
            )

    print(f"Total rows (team-user): {len(rows)}")
    print(f"Users with no SCIM match (email/name blank): {no_scim_match}")

    fieldnames = [
        "enterprise",
        "team_name",
        "team_slug",
        "login",
        "name",
        "email",
        "scim_userName",
        "copilot_assigned",
        "copilot_status",
        "plan_type",
        "last_activity_at",
        "active_status",
        "seat_created_at",
        "seat_updated_at",
    ]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"CSV report generated: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()