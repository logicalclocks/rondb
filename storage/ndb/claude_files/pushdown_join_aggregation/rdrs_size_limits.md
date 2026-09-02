# RDRS configurable request / response size limits

**Status: SHIPPED (2026-08-27, recorded green; full ronsql-suite regression pass).**

Motivated by the non-aggregate pushdown "result volume" risk item
(`non_aggregate_pushdown_plan.md`): projection-only RonSQL queries can
return arbitrarily many rows.  The engine drain streams, but RDRS
accumulates the whole HTTP body in memory before sending — an unbounded
response build is a self-DoS for a legitimate query.  This feature adds
a configurable response cap and makes the existing request cap actually
work.

## Request size: `Internal.maxReqSize` (existing key, fixed + validated)

Two previously inconsistent gates:

- **Drogon's built-in `clientMaxBodySize`** (default 1 MB,
  `HttpRequestParser.cc` ⇒ 413 + connection close) was never set by
  RDRS, so it SHADOWED the configurable `Internal.maxReqSize` (default
  4 MB, checked per-controller after full body assembly ⇒ 400).  A 2 MB
  POST got drogon's 413 and raising `maxReqSize` had no effect.
- Fix (`main.cc`, beside `setThreadNum`):
  `drogon::app().setClientMaxBodySize(globalConfigs.internal.maxReqSize)`
  — one coherent boundary.  Over-limit requests get drogon's 413
  (plain body) **before** assembly; the per-controller 400 checks stay
  as backstops (documented both shapes).
- `config_structs_def.hpp`: added the missing
  `PROBLEM(maxReqSize < 256, ...)` validation (the docstring promised
  it) and extended the docstring with the drogon-body-limit role plus
  the NumThreads × maxReqSize simdjson parse-buffer pre-allocation
  note (`json_parser.cpp`).
- `rdrs_dal.h`: `PAYLOAD_TOO_LARGE = 413` in `HTTP_CODE`;
  `error_strings.h`: `ERROR_REQUEST_TOO_LARGE` (112) for backstop
  messages.

## Response size: `Internal.MaxRespSize` (new key)

- One X-macro line in `config_structs_def.hpp`:
  `CM(Uint32, maxRespSize, MaxRespSize, 64 * 1024 * 1024, ...)` —
  bytes, **0 = unlimited**, enforced on the /ronsql endpoint (REST
  controller + MySQL-router handler).  `PROBLEM(maxRespSize != 0 &&
  maxRespSize < 65536, ...)`.  Uint32 deliberately (not Uint64):
  `Uint64` = `unsigned long long` vs `uint64_t` = `unsigned long` on
  Linux glibc are distinct types, so the X-macro parser overload
  dispatch is platform-risky; 4 GiB − 1 is far beyond any sane body.
- **`capped_ostream.hpp`** (new, server/src): `CappedStringBuf :
  std::streambuf` accumulating into a `std::string`, refusing writes
  past the cap in `xsputn`/`overflow` (returns failure ⇒ badbit ⇒
  later inserts silently no-op) with a latched `exceeded()` flag and
  a `take()` move-out; `CappedOStream : std::ostream` wraps it.
  **Deliberately non-throwing**: RonSQL never checks stream state and
  `ronsql_operation.cpp` has `catch (...) { abort(); }` — a throwing
  cap would take the server down.  Every printer writes via
  `operator<<` on `RonSQLExecParams::out_stream`
  (`std::basic_ostream<char>*`), so the streambuf intercepts 100% of
  engine output with zero engine changes.  `take()` into
  `setBody(std::string&&)` also removes one of the two full-body
  copies on the success path.
- `ronsql_ctrl.cpp` + `main.cc` (MySQL-router handler): the
  `std::ostringstream` becomes `CappedOStream(maxRespSize)` (prologue
  and epilogue go through it too, so the cap covers the full body);
  after the engine returns, `exceeded()` && would-be-200 ⇒ HTTP 500 +
  `ERROR_RESPONSE_TOO_LARGE` (113) message naming the cap value and
  suggesting LIMIT / raising `Internal.MaxRespSize`.  500, not 413:
  the request is well-formed — the server refuses the *build*.
- `ronsql_validate_and_init_params` signature relaxed
  `std::ostringstream*` → `std::ostream*`.

### Deliberate v1 limits (named follow-ups)

- The drain is **not aborted early** — past the cap the scan keeps
  streaming server-side into no-op writes; bounding memory is the
  goal.  Early abort needs engine-side stream-state checks.
- The **subquery inner ostringstream** (`RonSQLPreparer.cpp`, scalar
  subquery substitution) and the **scan/pk endpoints** (same
  accumulate-then-send shape in `scan_read_ctrl.cpp`) are not capped;
  the scan generalization is the natural next step.
- The Go config mirror (`server/test_go/.../structs.go`) was already
  stale (no MaxReqSize either) and was not extended — noted in the
  commit message.

## MTR

- `suite/ronsql/t/ronsql_size_limits.test` + `.cnf` +
  `suite/ronsql/rdrs_config_template_limits.json`: the cnf clones the
  suite my.cnf with `[rdrs.1.1]` pointed at the tiny-caps template
  (`maxReqSize` 300 / `MaxRespSize` 65536; rdrs.2.1 untouched).
  rl-1 413 on a >300-byte body; rl-2 200 on a small request (gate
  sane); rl-3 500 + "Response exceeds the configured MaxRespSize" +
  "Internal.MaxRespSize = 65536" greps on an ~82 KiB TSV result;
  rl-4 200 with `LIMIT 5` (constant column ⇒ deterministic body).
- `suite/ronsql_large/t/ronsql_large_passthrough.test` lp-5 is the
  big-but-legal control on the DEFAULT caps: the ~10 MB / 100,001-line
  lp-1 body accepted end-to-end.
