# passthrough_types family — findings

Phase 0b of `non_aggregate_phase_0.md` (pass-through printer type
coverage + ColumnMetadata).  Cases pt-1..6 lock in the fixed behavior;
before the fix, pt-1/2/3/5/6 failed with "Unsupported column type" and
pt-4 silently printed CTE temporal MIN/MAX outputs as raw packed
integers.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| BIT column projected in a pass-through result | `WITH cb AS (SELECT b_grp AS g, COUNT(*) AS n FROM bin1 GROUP BY b_grp) SELECT b.b_bits, cb.n FROM bin1 AS b JOIN cb ON cb.g = b.b_grp;` | rejection-assert (pt-P1) | BIT formatting unsupported in print_passthrough_value (also unsupported in the aggregate GROUP BY print) | body_passthrough_types.inc |
| VARBINARY column projected in a pass-through result | same with `b.b_raw` | rejection-assert (pt-P2) | binary formatting unsupported in print_passthrough_value | body_passthrough_types.inc |
