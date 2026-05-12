# Phase O: Typed-register overflow coverage

## Goal

Complete overflow and boundary coverage for the typed-register
interpreter arithmetic work.  The tests should prove the intended
register-machine semantics:

- integer arithmetic is checked at typed register width
- sub-BIGINT SQL column types are promoted before arithmetic
- divide and modulo reject zero divisors
- signed overflow is rejected
- unsigned and mixed signed/unsigned overflow is rejected
- bitwise and shift operations keep their existing error behavior for
  unsupported operands and invalid shift counts

## O.1: Expected semantics

Document and verify the intended semantics before extending tests:

- `TINYINT`, `SMALLINT`, `MEDIUMINT`, and `INT` values are loaded into
  typed 64-bit registers before arithmetic.
- Arithmetic overflow is therefore checked against `Int64` or `Uint64`
  register bounds, not the original SQL column width.
- Floating-point arithmetic follows `double` behavior unless a later
  phase explicitly decides to reject infinity or NaN.
- Branch NULL behavior remains out of scope for this phase.

## O.2: Signed integer overflow

Add focused signed-register tests for:

- `LLONG_MAX + 1`
- `LLONG_MIN - 1`
- `3037000500 * 3037000500`
- `LLONG_MIN / -1`
- `LLONG_MIN % -1`

The first three paths are already partly covered.  The immediate risk
is signed division and modulo with `LLONG_MIN` and `-1`, since plain C++
signed division/modulo can overflow there.  Fix the interpreter if
needed so these cases return the interpreter overflow error instead of
executing undefined or platform-dependent behavior.

## O.3: Unsigned integer overflow

Add tests for unsigned-register overflow and underflow:

- `UINT64_MAX + 1`
- `0 - 1`
- `UINT64_MAX * 2`

Verify that unsigned `DIV` and `MOD` only reject zero divisors and do
not otherwise overflow.

## O.4: Mixed signed/unsigned overflow

Add tests for mixed signed/unsigned arithmetic:

- unsigned max plus positive signed
- unsigned max minus negative signed
- unsigned high-bit multiplied by positive signed
- negative signed minus unsigned max
- signed negative divided or modulo unsigned zero

Verify that representable negative results become signed registers and
that results below `LLONG_MIN` or above `UINT64_MAX` return overflow.

## O.5: Const opcode variants

Add equivalent overflow/error tests for const forms:

- `ADD_REG_CONST`
- `SUB_REG_CONST`
- `MUL_REG_CONST`
- `DIV_REG_CONST`
- `MOD_REG_CONST`

Cover divide/modulo by zero if the NDB API helper can encode the
required immediate.  Include negative immediate behavior where the API
supports it.

## O.6: Width-promotion matrix

Add non-overflow boundary tests for all integer column widths:

- signed `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT`
- unsigned `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT`

Examples should prove that `TINYINT 127 + 1` succeeds as register
arithmetic and does not overflow at the original SQL type width.

## O.7: Shift and bitwise boundaries

Confirm or add tests for:

- shift by `64`
- negative shift count when constructible
- float operands rejected for bitwise and shift operations
- register-register and register-const shift variants

Bitwise operations do not have arithmetic overflow, so coverage should
focus on invalid operands and shift bounds.

## O.8: Floating arithmetic behavior

Add explicit tests documenting current floating behavior:

- large `DOUBLE + DOUBLE`
- large `DOUBLE * DOUBLE`
- divide by zero rejection

If floating overflow yields infinity, the test should document that as
accepted behavior unless a later design phase changes it.

## O.9: Coverage audit table

Add a compact audit table or comment near the typed-register tests that
lists coverage by operation:

- register-register
- register-const
- signed
- unsigned
- mixed signed/unsigned
- zero divisor
- overflow edge

This should make future interpreter opcode additions easier to audit.
