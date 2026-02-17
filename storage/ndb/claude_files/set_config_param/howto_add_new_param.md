# SET Config Param — How to Add a New Parameter

Only **2 files** need changes (the signal plumbing is already in place).

## Step 1: Cmvmi.cpp — Add Runtime Dispatch

File: `storage/ndb/src/kernel/blocks/cmvmi/Cmvmi.cpp`

In `Cmvmi::execSET_CONFIG_PARAM_REQ()`, add a new case to the `switch (configKey)` block. The case must apply the runtime effect by sending a signal to the appropriate block.

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

The ConfigValues update (for ndbinfo) happens automatically before the switch — no per-parameter code needed for that.

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

**e)** Update the error message in `executeSet()` that lists supported parameters.
