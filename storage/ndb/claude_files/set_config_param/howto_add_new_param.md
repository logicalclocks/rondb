# SET Config Param — How to Add a New Parameter

Only **2 files** need changes (the signal plumbing is already in place).

## Step 1: Cmvmi.cpp — Add Runtime Dispatch

File: `storage/ndb/src/kernel/blocks/cmvmi/Cmvmi.cpp`

In `Cmvmi::execSET_CONFIG_PARAM_REQ()`, add a new case to the `switch (configKey)` block. The case must apply the runtime effect by sending a signal to the appropriate block (or, for a node-global word such as the RONDB-1056 JIT mode, by calling its setter directly — see `case CFG_DB_COMPILED_INTERPRETER`, which validates the enum range and calls `dbtup_jit_set_mode()`).

```cpp
switch (configKey)
{
case CFG_DB_MAX_DISK_WRITE_SPEED:
{
  // ... existing case ...
  break;
}
case CFG_DB_YOUR_NEW_PARAM:
{
  jam();
  // Send signal to the block that owns this parameter at runtime.
  // Example: DumpStateOrd to the target block, or a dedicated signal.
  signal->theData[0] = DumpStateOrd::YourDumpCode;
  signal->theData[1] = Uint32(configValue >> 32);
  signal->theData[2] = Uint32(configValue & 0xFFFFFFFF);
  sendSignal(TARGET_BLOCK_REF, GSN_DUMP_STATE_ORD, signal, 3, JBB);
  break;
}
default:
  // ...
}
```

The ConfigValues update (for ndbinfo) happens automatically after the switch — no per-parameter code needed for that. It writes the value back with the entry's stored type (`Uint32` for CI_INT/CI_BOOL/CI_ENUM, `Uint64` for CI_INT64; `ConfigSection::set` rejects a type change), so 32-bit parameters work without special handling.

A case that rejects the value should send `SET_CONFIG_PARAM_REF` (`SetConfigParamRef`, the mgm server maps any REF to error 5070) and `return` from inside the switch, which skips the ConfigValues update so ndbinfo never shows a value that was not applied — `case CFG_DB_COMPILED_INTERPRETER` is the example.

If no runtime dispatch is needed (config only takes effect after restart), you can add an empty case with a log message.

## Step 2: CommandInterpreter.cpp — Add Client-Side Command

File: `storage/ndb/src/mgmclient/CommandInterpreter.cpp`

**a)** Add a new `executeSetYourParam()` method declaration near the existing ones:

```cpp
int executeSetYourParam(int processId, const char *value_str, bool all);
```

**b)** Add a case in `executeSet()` to dispatch the new parameter name:

```cpp
if (native_strcasecmp(param_name, "MaxDiskWriteSpeed") == 0)
{
  return executeSetMaxDiskWriteSpeed(processId, value_str, all);
}
else if (native_strcasecmp(param_name, "YourParamName") == 0)
{
  return executeSetYourParam(processId, value_str, all);
}
```

**c)** Implement `executeSetYourParam()` following the `executeSetMaxDiskWriteSpeed()` pattern:

1. Parse and validate the value (use `parse_size_value()` for byte values with K/M/G suffixes, or write custom parsing)
2. Validate range against ConfigInfo.cpp limits
3. Get config via `ndb_mgm_get_configuration()`, update the key, save via `ndb_mgm_set_configuration()`
4. Call `ndb_mgm_set_config_param(handle, nodeId, CFG_DB_YOUR_PARAM, value)` for runtime update
5. Print success/failure message

**d)** Update the `helpTextSet` string to list the new parameter.

**e)** Update the error message in `executeSet()` that lists supported parameters (three occurrences), and the matching `Supported parameters:` lines in `mysql-test/suite/ndb/r/ndb_config_set.result`.

**f)** Add MTR coverage: extend `mysql-test/suite/ndb/t/ndb_config_set.test` (checks the value through `ndbinfo.config_values` by parameter id), or a dedicated test when the runtime effect needs more setup (`ndb_set_compiled_interpreter.test` asserts the JIT effect via `ndbinfo.jit`).
