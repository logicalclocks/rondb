"""T2 recording harness: helpers to drive the live Hopsworks cluster and
record sharing behavior. See fg_fv_sharing_fine_grained_test_design.md §3.

Credentials come from the untracked env file server/api_keys (source it, or
run via t2_scenarios.py which loads it). Recordings deliberately contain no
secrets.
"""

import json
import os
import re
import subprocess
import urllib3
from datetime import datetime, timezone

import requests

urllib3.disable_warnings()

HOST = "10.115.253.130"
API = f"https://{HOST}/hopsworks-api/api"
RDRS = "https://10.115.253.124:4406/0.1.0"  # prod RDRS (current, pre-fix)
HOPSWORKS_VERSION = "5.0.1"

# T1-created entities (ids are stable for this cluster build)
PROJECT_IDS = {f"user{u}_project": 100000 + i
               for i, u in enumerate("abcdef")}
USERA_PROJECT_ID = PROJECT_IDS["usera_project"]
USERA_FS_ID = 100000
FG_IDS = {"usera_customers_fg": 100000, "usera_transactions_fg": 100001}
ONLINE_DB = "usera_project"
ONLINE_TABLES = {"usera_customers_fg": "usera_customers_fg_1",
                 "usera_transactions_fg": "usera_transactions_fg_1"}
FVS = {
    "full": ("usera_customers_transactions_fv", 1),
    "narrow": ("usera_txncount_fv", 1),
}

SHARING_TABLES = [
    "shared_feature_store", "shared_feature_group", "shared_feature",
    "restricted_feature_group_access", "restricted_feature_access",
]


def login_project(user):
    """The project a user logs in through: consumers use their own project;
    restricted users (j/k/l) have none and read in-project via their
    usera_project membership."""
    if user in ("j", "k", "l"):
        return "usera_project", USERA_PROJECT_ID
    return f"user{user}_project", PROJECT_IDS[f"user{user}_project"]


def api_key(user):
    key = os.environ.get(f"USER{user.upper()}_API_KEY")
    if not key:
        raise SystemExit(f"USER{user.upper()}_API_KEY not set - source api_keys")
    return key


def rest(user, method, path, **kw):
    """REST call as a user, authenticated by their api key."""
    r = requests.request(method, f"{API}{path}", verify=False,
                         headers={"Authorization": f"ApiKey {api_key(user)}"},
                         timeout=60, **kw)
    return r


def outcome_of(resp):
    """Normalize a requests.Response into a recording outcome dict."""
    out = {"status": resp.status_code}
    try:
        body = resp.json()
        for k in ("errorCode", "errorMsg", "usrMsg", "devMsg", "code", "message"):
            if isinstance(body, dict) and k in body:
                out[k] = body[k]
        if resp.ok:
            out["body"] = body
    except ValueError:
        out["body_text"] = resp.text[:500]
    return out


# ---------------- grants (always as usera, the data owner) ----------------

def share_store(grantee_project_id):
    return rest("a", "POST",
                f"/project/{USERA_PROJECT_ID}/featurestores/share"
                f"?project={grantee_project_id}")


def unshare_store(grantee_project_id):
    return rest("a", "DELETE",
                f"/project/{USERA_PROJECT_ID}/featurestores/share"
                f"?project={grantee_project_id}")


def share_fg(fg_name, grantee_project_id, features=()):
    q = f"?project={grantee_project_id}"
    for f in features:
        q += f"&feature={f}"
    return rest("a", "POST",
                f"/project/{USERA_PROJECT_ID}/featurestores/share"
                f"/featuregroups/{FG_IDS[fg_name]}{q}")


def unshare_fg(fg_name, grantee_project_id):
    return rest("a", "DELETE",
                f"/project/{USERA_PROJECT_ID}/featurestores/share"
                f"/featuregroups/{FG_IDS[fg_name]}?project={grantee_project_id}")


def grant_restricted(fg_name, email, features=()):
    q = f"?user={email}"
    for f in features:
        q += f"&feature={f}"
    return rest("a", "POST",
                f"/project/{USERA_PROJECT_ID}/featurestores/{USERA_FS_ID}"
                f"/featuregroups/{FG_IDS[fg_name]}/restrictedaccess{q}")


def revoke_restricted(fg_name, email):
    return rest("a", "DELETE",
                f"/project/{USERA_PROJECT_ID}/featurestores/{USERA_FS_ID}"
                f"/featuregroups/{FG_IDS[fg_name]}/restrictedaccess?user={email}")


# ---------------- metadata dump (via kubectl -> mysqld pod) ----------------

KUBECONFIG = "/Users/salman/code/hops/helm/kubeconfig.yml"


def dump_sharing_tables():
    """Return {table: [row-dicts]} for the five sharing tables."""
    result = {}
    for t in SHARING_TABLES:
        sql = f"SELECT * FROM hopsworks.{t}"
        cmd = ["kubectl", "--kubeconfig", KUBECONFIG, "-n", "hopsworks",
               "exec", "mysqlds-0", "--", "sh", "-lc",
               '/srv/hops/mysql/bin/mysql --defaults-file=/srv/hops/mysql-cluster/my.cnf'
               ' -u root -p"$MYSQL_ROOT_PASSWORD" -S /srv/hops/mysql-cluster/mysql.sock'
               f' -B -e "{sql}"']
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        lines = [ln for ln in out.stdout.splitlines()
                 if ln and not ln.startswith("Defaulted")]
        if not lines:
            result[t] = []
            continue
        headers = lines[0].split("\t")
        result[t] = [dict(zip(headers, ln.split("\t"))) for ln in lines[1:]]
    return result


# ---------------- reads ----------------

def hsfs_read_fv(user, fv_key, entry={"customer_id": 1}):
    """Read one feature vector via the hopsworks python client as `user`,
    from THEIR own project, accessing usera's (possibly shared) store.
    Returns an outcome dict; on exception captures type/status/message.
    Runs in-process; hsfs caches connections per (host, project, key) so we
    isolate by subprocess to keep recordings clean."""
    name, version = FVS[fv_key]
    code = f"""
import json, os, sys, warnings
warnings.filterwarnings("ignore")
import hopsworks
out = {{}}
try:
    project = hopsworks.login(host="{HOST}", port=443,
        project=os.environ["T2_PROJECT"], api_key_value=os.environ["T2_KEY"])
    fs = project.get_feature_store(name="usera_project_featurestore")
    out["get_feature_store"] = "ok"
    fv = fs.get_feature_view("{name}", {version})
    out["get_feature_view"] = "ok"
    vec = fv.get_feature_vector({json.dumps(entry)})
    out["get_feature_vector"] = "ok"
    out["vector"] = [str(v) for v in vec] if vec is not None else None
except Exception as e:
    out["exception"] = type(e).__name__
    msg = str(e)
    out["message"] = msg[:1000]
    for attr in ("response",):
        r = getattr(e, attr, None)
        if r is not None and hasattr(r, "status_code"):
            out["status"] = r.status_code
print("T2JSON:" + json.dumps(out))
"""
    own_project, _ = login_project(user)
    env = dict(os.environ,
               T2_PROJECT=own_project,
               T2_KEY=api_key(user))
    try:
        p = subprocess.run(
            ["/Users/salman/code/hops/fg_test_env/bin/python", "-c", code],
            capture_output=True, text=True, timeout=120, env=env)
        stdout, stderr = p.stdout, p.stderr
    except subprocess.TimeoutExpired as e:
        # Known hsfs client bug: on some online-store SQL errors the async
        # executor swallows the exception and the client hangs. The real
        # error is on stderr - salvage it. (TimeoutExpired returns bytes
        # even when the run was text-mode.)
        def _s(x):
            return x.decode(errors="replace") if isinstance(x, bytes) else (x or "")
        stdout = _s(e.stdout)
        stderr = _s(e.stderr)
        errline = next((ln for ln in reversed(stderr.splitlines())
                        if "Error" in ln or "error" in ln), "")
        return {"exception": "ClientHang",
                "message": "hsfs client hung (known client bug on this "
                           "error path); underlying error: " + errline[:500]}
    m = re.search(r"T2JSON:(\{.*\})", stdout)
    if m:
        return json.loads(m.group(1))
    return {"harness_error": (stdout + stderr)[-1000:]}


def rdrs_feature_store(user, fv_key, entry={"customer_id": 1}, batch=False):
    """Read via the PROD RDRS REST API (current member-only code — records
    the pre-fix gap, not the target behavior)."""
    name, version = FVS[fv_key]
    if batch:
        body = {"featureStoreName": ONLINE_DB, "featureViewName": name,
                "featureViewVersion": version, "entries": [entry]}
        path = "/batch_feature_store"
    else:
        body = {"featureStoreName": ONLINE_DB, "featureViewName": name,
                "featureViewVersion": version, "entries": entry}
        path = "/feature_store"
    try:
        r = requests.post(f"{RDRS}{path}", json=body, verify=False, timeout=30,
                          headers={"X-API-KEY": api_key(user)})
        return outcome_of(r)
    except requests.RequestException as e:
        return {"harness_error": str(e)[:300]}


def rdrs_pk_read(user, fg_name, pk=1):
    body = {"filters": [{"column": "customer_id", "value": pk}]}
    table = ONLINE_TABLES[fg_name]
    try:
        r = requests.post(f"{RDRS}/{ONLINE_DB}/{table}/pk-read", json=body,
                          verify=False, timeout=30,
                          headers={"X-API-KEY": api_key(user)})
        return outcome_of(r)
    except requests.RequestException as e:
        return {"harness_error": str(e)[:300]}


def rdrs_batch_pk_read(user, fg_name, pks=(1, 2)):
    table = ONLINE_TABLES[fg_name]
    ops = [{"method": "POST",
            "relative-url": f"{ONLINE_DB}/{table}/pk-read",
            "body": {"filters": [{"column": "customer_id", "value": pk}]}}
           for pk in pks]
    try:
        r = requests.post(f"{RDRS}/batch", json={"operations": ops},
                          verify=False, timeout=30,
                          headers={"X-API-KEY": api_key(user)})
        return outcome_of(r)
    except requests.RequestException as e:
        return {"harness_error": str(e)[:300]}


def hsfs_create_own_fv(user, fv_name, customers_cols, transactions_cols):
    """As `user`, create an FV named fv_name in THEIR login project's own
    feature store, built on usera's (shared/restricted) FGs with the given
    column selections, then immediately read a vector from it. Captures
    the outcome of each step (get FGs, build query, create FV, read)."""
    code = f"""
import json, os, warnings
warnings.filterwarnings("ignore")
import hopsworks
out = {{}}
try:
    project = hopsworks.login(host="{HOST}", port=443,
        project=os.environ["T2_PROJECT"], api_key_value=os.environ["T2_KEY"])
    shared_fs = project.get_feature_store(name="usera_project_featurestore")
    cust = shared_fs.get_feature_group("usera_customers_fg", 1)
    tcols = {transactions_cols!r}
    # fetch the transactions FG only if the FV uses it (fetching an
    # ungranted FG's metadata is itself denied for restricted users)
    trans = shared_fs.get_feature_group("usera_transactions_fg", 1) if tcols else None
    out["get_feature_groups"] = "ok"
    query = cust.select({customers_cols!r})
    if tcols:
        query = query.join(trans.select(tcols), on=["customer_id"])
    out["build_query"] = "ok"
    own_fs = project.get_feature_store()
    out["own_feature_store"] = own_fs.name
    fv = own_fs.get_or_create_feature_view(
        name="{fv_name}", version=1,
        description="consumer-side FV over usera FGs (T2 scenario F)",
        query=query, labels=[])
    out["create_feature_view"] = "ok"
    vec = fv.get_feature_vector({{"customer_id": 1}})
    out["get_feature_vector"] = "ok"
    out["vector"] = [str(v) for v in vec] if vec is not None else None
except Exception as e:
    out["exception"] = type(e).__name__
    out["message"] = str(e)[:1000]
    r = getattr(e, "response", None)
    if r is not None and hasattr(r, "status_code"):
        out["status"] = r.status_code
print("T2JSON:" + json.dumps(out))
"""
    own_project, _ = login_project(user)
    env = dict(os.environ, T2_PROJECT=own_project, T2_KEY=api_key(user))
    try:
        p = subprocess.run(
            ["/Users/salman/code/hops/fg_test_env/bin/python", "-c", code],
            capture_output=True, text=True, timeout=180, env=env)
        stdout, stderr = p.stdout, p.stderr
    except subprocess.TimeoutExpired as e:
        def _s(x):
            return x.decode(errors="replace") if isinstance(x, bytes) else (x or "")
        stdout, stderr = _s(e.stdout), _s(e.stderr)
        errline = next((ln for ln in reversed(stderr.splitlines())
                        if "Error" in ln or "error" in ln), "")
        return {"exception": "ClientHang",
                "message": "hsfs client hung; underlying error: " + errline[:500]}
    m = re.search(r"T2JSON:(\{.*\})", stdout)
    if m:
        return json.loads(m.group(1))
    return {"harness_error": (stdout + stderr)[-1000:]}


def rdrs_read_own_fv(user, store_name, fv_name, entry={"customer_id": 1}):
    """Read a consumer-side FV through prod RDRS: featureStoreName is the
    CONSUMER's own store even though the FV joins usera's FGs."""
    body = {"featureStoreName": store_name, "featureViewName": fv_name,
            "featureViewVersion": 1, "entries": entry}
    try:
        r = requests.post(f"{RDRS}/feature_store", json=body, verify=False,
                          timeout=30, headers={"X-API-KEY": api_key(user)})
        return outcome_of(r)
    except requests.RequestException as e:
        return {"harness_error": str(e)[:300]}


MYSQL_LB = "10.115.253.126"  # mysqld-external LoadBalancer


def mysql_grants_probe(user):
    """The decisive probe: fetch the user's per-user online-store
    credentials via their storage connector for usera's feature store,
    SHOW GRANTS, and try SELECTs on both FG tables plus a shared-column /
    unshared-column pair (detects table- vs column-level GRANTs)."""
    _, pid = login_project(user)
    r = rest(user, "GET",
             f"/project/{pid}/featurestores/{USERA_FS_ID}"
             f"/storageconnectors/onlinefeaturestore")
    if not r.ok:
        return {"connector": outcome_of(r)}
    args = {a["name"]: a["value"] for a in r.json().get("arguments", [])}
    acct, pw = args.get("user"), args.get("password")
    result = {"mysql_account": acct, "grants": [], "queries": {}}

    def q(label, sql):
        p = subprocess.run(
            ["mysql", "-h", MYSQL_LB, "-P", "3306", "-u", acct, f"-p{pw}",
             "-B", "-e", sql],
            capture_output=True, text=True, timeout=30)
        out = "\n".join(ln for ln in (p.stdout + p.stderr).splitlines()
                        if "Using a password" not in ln)
        result["queries"][label] = {"ok": p.returncode == 0,
                                    "output": out[:300]}

    p = subprocess.run(
        ["mysql", "-h", MYSQL_LB, "-P", "3306", "-u", acct, f"-p{pw}",
         "-B", "-e", "SHOW GRANTS"],
        capture_output=True, text=True, timeout=30)
    result["grants"] = [ln for ln in p.stdout.splitlines()
                        if ln.startswith("GRANT")]
    q("customers_all_cols",
      f"SELECT * FROM {ONLINE_DB}.{ONLINE_TABLES['usera_customers_fg']}"
      " WHERE customer_id=1")
    q("transactions_all_cols",
      f"SELECT * FROM {ONLINE_DB}.{ONLINE_TABLES['usera_transactions_fg']}"
      " WHERE customer_id=1")
    q("transactions_shared_col",
      "SELECT customer_id, num_transactions_30d FROM "
      f"{ONLINE_DB}.{ONLINE_TABLES['usera_transactions_fg']} WHERE customer_id=1")
    q("transactions_unshared_col",
      "SELECT customer_id, total_spend_30d FROM "
      f"{ONLINE_DB}.{ONLINE_TABLES['usera_transactions_fg']} WHERE customer_id=1")
    return result


# ---------------- recording ----------------

RECORDING_DIR = os.path.dirname(os.path.abspath(__file__))


def write_recording(scenario, data):
    from t2_descriptions import READS_LEGEND, SCENARIO_CONTEXT, SETUP
    data = {"scenario": scenario,
            "generated": datetime.now(timezone.utc).isoformat(),
            "hopsworks_version": HOPSWORKS_VERSION,
            "generator": "t2_scenarios.py",
            "setup": SETUP,
            "context": SCENARIO_CONTEXT.get(scenario, ""),
            "reads_legend": READS_LEGEND,
            **data}
    path = os.path.join(RECORDING_DIR, f"recording_{scenario}.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"recorded -> {os.path.basename(path)}")
    return path


def standard_reads(user):
    """The read battery every scenario runs: both FVs and both tables,
    through hsfs (reference) and prod RDRS (gap documentation)."""
    return {
        "hsfs_full_fv": hsfs_read_fv(user, "full"),
        "hsfs_narrow_fv": hsfs_read_fv(user, "narrow"),
        "rdrs_full_fv": rdrs_feature_store(user, "full"),
        "rdrs_narrow_fv": rdrs_feature_store(user, "narrow"),
        "rdrs_batch_full_fv": rdrs_feature_store(user, "full", batch=True),
        "rdrs_pkread_customers": rdrs_pk_read(user, "usera_customers_fg"),
        "rdrs_pkread_transactions": rdrs_pk_read(user, "usera_transactions_fg"),
        "rdrs_batch_pkread_customers": rdrs_batch_pk_read(user, "usera_customers_fg"),
        "mysql_grants_probe": mysql_grants_probe(user),
    }
