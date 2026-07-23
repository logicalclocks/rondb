"""T2 scenario recorder. Run: python t2_scenarios.py A2 A1 A3 ...

Each scenario applies its grant(s) as usera, dumps the five sharing
tables, performs the standard read battery as the designated reader
(hsfs reference + prod RDRS), and writes recording_<scenario>.json.

Scenario/grant map (design doc §1/§2):
  A: userb  = entire store shared
  B: userc  = both FGs whole;  userd = customers_fg whole only
  C: usere  = customers whole + transactions{num_transactions_30d}
  D: userj  = restricted, no grants; userk = customers full;
     userl  = customers full + transactions partial
  userf = outsider, never granted anything
"""

import sys

import t2_lib as t2
from t2_lib import (FG_IDS, PROJECT_IDS, dump_sharing_tables, grant_restricted,
                    outcome_of, revoke_restricted, share_fg, share_store,
                    standard_reads, unshare_fg, unshare_store, write_recording)

USERB_P = PROJECT_IDS["userb_project"]
USERC_P = PROJECT_IDS["userc_project"]
USERD_P = PROJECT_IDS["userd_project"]
USERE_P = PROJECT_IDS["usere_project"]


def scenario_A2():
    """Outsider userf: no grant, all reads must deny."""
    write_recording("A2", {
        "description": "userf has no grant at all - store-level denial on every surface",
        "grants": [],
        "sharing_tables": dump_sharing_tables(),
        "reads": {"userf": standard_reads("f")},
    })


def scenario_A1():
    """Share usera's entire store with userb_project; userb reads all."""
    g = share_store(USERB_P)
    write_recording("A1", {
        "description": "entire store shared with userb_project (static grant)",
        "grants": [{"action": "share_store", "grantee": "userb_project",
                    "result": outcome_of(g)}],
        "sharing_tables": dump_sharing_tables(),
        "reads": {"userb": standard_reads("b")},
    })


def scenario_A3():
    """Live transition: unshare the store from userb, verify denial,
    re-share to restore the static state, verify allow again."""
    g1 = unshare_store(USERB_P)
    tables_after_unshare = dump_sharing_tables()
    reads_after_unshare = standard_reads("b")
    g2 = share_store(USERB_P)
    reads_after_reshare = standard_reads("b")
    write_recording("A3", {
        "description": "unshare store from userb_project -> deny; re-share -> allow",
        "grants": [{"action": "unshare_store", "grantee": "userb_project",
                    "result": outcome_of(g1)},
                   {"action": "share_store", "grantee": "userb_project",
                    "result": outcome_of(g2)}],
        "sharing_tables_after_unshare": tables_after_unshare,
        "sharing_tables": dump_sharing_tables(),
        "reads_after_unshare": {"userb": reads_after_unshare},
        "reads_after_reshare": {"userb": reads_after_reshare},
    })


def scenario_B1():
    """userd: customers_fg shared whole (and nothing else).
    One recording covers design rows B1 (grant metadata), B2 (full FV
    denied) and B4 (placeholder row opens nothing: narrow FV + transactions
    table reads must deny too)."""
    g = share_fg("usera_customers_fg", USERD_P)
    write_recording("B1_B2_B4", {
        "description": "customers_fg shared whole with userd_project only; "
                       "full FV and anything touching transactions_fg must deny",
        "grants": [{"action": "share_fg", "fg": "usera_customers_fg",
                    "grantee": "userd_project", "features": [],
                    "result": outcome_of(g)}],
        "sharing_tables": dump_sharing_tables(),
        "reads": {"userd": standard_reads("d")},
    })


def scenario_B3():
    """userc: BOTH FGs shared whole (no store share) - full FV allowed via
    the per-FG cascade."""
    g1 = share_fg("usera_customers_fg", USERC_P)
    g2 = share_fg("usera_transactions_fg", USERC_P)
    write_recording("B3", {
        "description": "both FGs shared whole with userc_project - "
                       "everything readable via per-FG cascade",
        "grants": [{"action": "share_fg", "fg": "usera_customers_fg",
                    "grantee": "userc_project", "features": [],
                    "result": outcome_of(g1)},
                   {"action": "share_fg", "fg": "usera_transactions_fg",
                    "grantee": "userc_project", "features": [],
                    "result": outcome_of(g2)}],
        "sharing_tables": dump_sharing_tables(),
        "reads": {"userc": standard_reads("c")},
    })


def scenario_B5():
    """Live transition: unshare customers_fg from userc -> full FV breaks;
    re-share -> restored."""
    g1 = unshare_fg("usera_customers_fg", USERC_P)
    tables_after_unshare = dump_sharing_tables()
    reads_after_unshare = standard_reads("c")
    g2 = share_fg("usera_customers_fg", USERC_P)
    reads_after_reshare = standard_reads("c")
    write_recording("B5", {
        "description": "unshare one of userc's two FGs -> full FV re-breaks; "
                       "re-share restores",
        "grants": [{"action": "unshare_fg", "fg": "usera_customers_fg",
                    "grantee": "userc_project", "result": outcome_of(g1)},
                   {"action": "share_fg", "fg": "usera_customers_fg",
                    "grantee": "userc_project", "features": [],
                    "result": outcome_of(g2)}],
        "sharing_tables_after_unshare": tables_after_unshare,
        "sharing_tables": dump_sharing_tables(),
        "reads_after_unshare": {"userc": reads_after_unshare},
        "reads_after_reshare": {"userc": reads_after_reshare},
    })


def scenario_C():
    """usere: customers_fg whole + transactions_fg restricted to
    num_transactions_30d. Covers design rows C1 (narrow FV allowed),
    C2 (full FV denied), C3 (shared_feature rows incl. PK/event_time
    force-inclusion) and C4 (column-level MySQL grant probe)."""
    g1 = share_fg("usera_customers_fg", USERE_P)
    g2 = share_fg("usera_transactions_fg", USERE_P,
                  features=["num_transactions_30d"])
    write_recording("C", {
        "description": "usere: customers whole + transactions{num_transactions_30d}; "
                       "narrow FV allowed, full FV denied, column-level online grant",
        "grants": [{"action": "share_fg", "fg": "usera_customers_fg",
                    "grantee": "usere_project", "features": [],
                    "result": outcome_of(g1)},
                   {"action": "share_fg", "fg": "usera_transactions_fg",
                    "grantee": "usere_project",
                    "features": ["num_transactions_30d"],
                    "result": outcome_of(g2)}],
        "sharing_tables": dump_sharing_tables(),
        "reads": {"usere": standard_reads("e")},
    })


def scenario_D0():
    """userj: restricted member of usera_project, NO grants - must see
    nothing (also the acceptance test for today's RDRS gap: prod RDRS will
    ALLOW because userj is a project_team member)."""
    write_recording("D0", {
        "description": "restricted member without grants sees nothing "
                       "(reads in-project via usera_project membership)",
        "grants": [],
        "sharing_tables": dump_sharing_tables(),
        "reads": {"userj": standard_reads("j")},
    })


def scenario_D2():
    """userk: restricted member granted customers_fg in FULL."""
    g = grant_restricted("usera_customers_fg", "userk@lc.com")
    write_recording("D2", {
        "description": "restricted userk granted customers_fg entirely; "
                       "full FV must deny (transactions not granted)",
        "grants": [{"action": "grant_restricted", "fg": "usera_customers_fg",
                    "user": "userk@lc.com", "features": [],
                    "result": outcome_of(g)}],
        "sharing_tables": dump_sharing_tables(),
        "reads": {"userk": standard_reads("k")},
    })


def scenario_D1():
    """Live transition: temporarily grant userk transactions_fg in full too
    -> full FV allowed; revoke to restore the D2 static state."""
    g1 = grant_restricted("usera_transactions_fg", "userk@lc.com")
    tables_full = dump_sharing_tables()
    reads_full = standard_reads("k")
    g2 = revoke_restricted("usera_transactions_fg", "userk@lc.com")
    reads_after_revoke = standard_reads("k")
    write_recording("D1", {
        "description": "userk temporarily granted BOTH FGs entirely -> all "
                       "granted; revoke transactions -> back to D2 state",
        "grants": [{"action": "grant_restricted", "fg": "usera_transactions_fg",
                    "user": "userk@lc.com", "features": [],
                    "result": outcome_of(g1)},
                   {"action": "revoke_restricted", "fg": "usera_transactions_fg",
                    "user": "userk@lc.com", "result": outcome_of(g2)}],
        "sharing_tables_while_granted": tables_full,
        "sharing_tables": dump_sharing_tables(),
        "reads_while_granted": {"userk": reads_full},
        "reads_after_revoke": {"userk": reads_after_revoke},
    })


def scenario_D3():
    """userl: restricted member granted customers_fg full +
    transactions_fg restricted to num_transactions_30d."""
    g1 = grant_restricted("usera_customers_fg", "userl@lc.com")
    g2 = grant_restricted("usera_transactions_fg", "userl@lc.com",
                          features=["num_transactions_30d"])
    write_recording("D3", {
        "description": "restricted userl: customers full + transactions "
                       "{num_transactions_30d}; narrow FV allowed, full denied",
        "grants": [{"action": "grant_restricted", "fg": "usera_customers_fg",
                    "user": "userl@lc.com", "features": [],
                    "result": outcome_of(g1)},
                   {"action": "grant_restricted", "fg": "usera_transactions_fg",
                    "user": "userl@lc.com",
                    "features": ["num_transactions_30d"],
                    "result": outcome_of(g2)}],
        "sharing_tables": dump_sharing_tables(),
        "reads": {"userl": standard_reads("l")},
    })


def scenario_D4():
    """Granting restricted access to a user WITHOUT the restricted role
    (userb, not even a member) must be rejected."""
    g = grant_restricted("usera_customers_fg", "userb@lc.com")
    write_recording("D4", {
        "description": "restricted grant to non-restricted user rejected",
        "grants": [{"action": "grant_restricted", "fg": "usera_customers_fg",
                    "user": "userb@lc.com", "features": [],
                    "result": outcome_of(g)}],
        "sharing_tables": dump_sharing_tables(),
        "reads": {},
    })


def scenario_F1():
    """userc (both FGs shared whole) creates his OWN FV in userc_project
    joining both usera FGs with all columns, then reads it."""
    create = t2.hsfs_create_own_fv(
        "c", "userc_own_fv",
        ["customer_id", "age", "country", "is_premium"],
        ["num_transactions_30d", "total_spend_30d", "avg_transaction_value_30d"])
    write_recording("F1", {
        "create_own_fv": create,
        "rdrs_read_own_fv": t2.rdrs_read_own_fv("c", "userc_project",
                                                "userc_own_fv"),
        "sharing_tables": dump_sharing_tables(),
    })


def scenario_F2_F3():
    """usere (customers whole + transactions{num_transactions_30d}):
    F2 = own FV within the grant (must work); F3 = own FV selecting the
    UNSHARED total_spend_30d (record where it fails)."""
    ok = t2.hsfs_create_own_fv(
        "e", "usere_own_fv",
        ["customer_id", "age"],
        ["num_transactions_30d"])
    denied = t2.hsfs_create_own_fv(
        "e", "usere_own_denied_fv",
        ["customer_id", "age"],
        ["total_spend_30d"])
    write_recording("F2_F3", {
        "create_own_fv_within_grant": ok,
        "rdrs_read_own_fv": t2.rdrs_read_own_fv("e", "usere_project",
                                                "usere_own_fv"),
        "create_own_fv_unshared_column": denied,
        "sharing_tables": dump_sharing_tables(),
    })


def scenario_F4_F5():
    """Restricted users creating FVs in usera_project itself (they own no
    project): F4 = userk (customers full) over customers only; F5 = userl
    (partial) within grant and over the ungranted total_spend_30d."""
    f4 = t2.hsfs_create_own_fv(
        "k", "userk_own_fv",
        ["customer_id", "age"], [])
    f5_ok = t2.hsfs_create_own_fv(
        "l", "userl_own_fv",
        ["customer_id", "age"],
        ["num_transactions_30d"])
    f5_denied = t2.hsfs_create_own_fv(
        "l", "userl_own_denied_fv",
        ["customer_id", "age"],
        ["total_spend_30d"])
    write_recording("F4_F5", {
        "userk_create_fv_customers_only": f4,
        "userk_rdrs_read": t2.rdrs_read_own_fv("k", "usera_project",
                                               "userk_own_fv"),
        "userl_create_fv_within_grant": f5_ok,
        "userl_rdrs_read": t2.rdrs_read_own_fv("l", "usera_project",
                                               "userl_own_fv"),
        "userl_create_fv_ungranted_column": f5_denied,
        "sharing_tables": dump_sharing_tables(),
    })


SCENARIOS = {name[len("scenario_"):]: fn
             for name, fn in list(globals().items())
             if name.startswith("scenario_")}


def main():
    args = sys.argv[1:]
    if not args or any(a not in SCENARIOS for a in args):
        sys.exit(f"usage: t2_scenarios.py <{'|'.join(sorted(SCENARIOS))}> ...")
    for a in args:
        print(f"=== scenario {a} ===")
        SCENARIOS[a]()


if __name__ == "__main__":
    main()
