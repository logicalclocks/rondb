# CLAUDE.md — NDB Storage Engine

## Adding a New Runtime-Settable Configuration Parameter

The `SET` command in the MGM client allows changing configuration parameters on running data nodes. It updates the persistent configuration, the runtime value, and ndbinfo reporting — all in one command.

**Reference implementation**: `MaxDiskWriteSpeed` (RONDB-1017).

### Architecture

```
MGM Client (CommandInterpreter.cpp)
  1. Parse command, validate value
  2. Update permanent config via ndb_mgm_set_configuration()
  3. Send runtime signal via ndb_mgm_set_config_param()
       |
MGM API (mgmapi.cpp)
  -> ndb_mgm_call("set_config_param", ...)
       |
MGM Server (Services.cpp -> MgmtSrvr.cpp)
  -> MgmApiSession::set_config_param()
  -> MgmtSrvr::set_config_param_request()
  -> Creates SetConfigParamReq signal, sends to target data node(s)
  -> Waits for CONF/REF responses
       |
Data Node CMVMI (Cmvmi.cpp)
  1. Updates ConfigValues (for ndbinfo reporting)
  2. Dispatches to appropriate block via switch(configKey)
  3. Sends SetConfigParamConf back to MgmtSrvr
```

The signal infrastructure (`SetConfigParam`) is **already implemented** and generic — it carries a config key (Uint32) and a value (Uint64 split into two Uint32 words). Adding a new parameter only requires changes in two files.

### Steps to Add a New Parameter

Only **2 files** need changes for a new parameter (the signal plumbing is already in place):

#### Step 1: Cmvmi.cpp — Add Runtime Dispatch

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

#### Step 2: CommandInterpreter.cpp — Add Client-Side Command

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

### Key Reference Points

| What | File | Look for |
|------|------|----------|
| Config key constants | `include/mgmapi/mgmapi_config_parameters.h` | `CFG_DB_*` |
| Config metadata (type, range, default) | `src/common/mgmcommon/ConfigInfo.cpp` | Parameter name string |
| DUMP state codes | `include/kernel/signaldata/DumpStateOrd.hpp` | `DumpStateOrd` enum |
| Signal definition | `include/kernel/signaldata/SetConfigParam.hpp` | SetConfigParamReq/Conf/Ref |
| Signal numbers (GSN) | `include/kernel/GlobalSignalNumbers.h` | `GSN_SET_CONFIG_PARAM_*` |
| Cmvmi handler | `src/kernel/blocks/cmvmi/Cmvmi.cpp` | `execSET_CONFIG_PARAM_REQ` |
| Client SET command | `src/mgmclient/CommandInterpreter.cpp` | `executeSet`, `executeSetMaxDiskWriteSpeed` |
| Value parser (K/M/G) | `src/mgmclient/CommandInterpreter.cpp` | `parse_size_value` |
| MgmtSrvr handler | `src/mgmsrv/MgmtSrvr.cpp` | `set_config_param_request` |
| Services handler | `src/mgmsrv/Services.cpp` | `set_config_param` |
| mgmapi function | `src/mgmapi/mgmapi.cpp` | `ndb_mgm_set_config_param` |
| Error code | `src/mgmsrv/ndb_mgmd_error.h` | `FAILED_SET_CONFIG_PARAM_REQUEST` |
| Error text | `src/ndbapi/ndberror.cpp` | `5070` |

All paths are relative to `storage/ndb/`.

### Files That Do NOT Need Changes for New Parameters

These files contain the generic signal infrastructure and are already complete:

- `SetConfigParam.hpp` — signal definition (generic key + Uint64 value)
- `GlobalSignalNumbers.h` — GSN numbers already allocated
- `SignalNames.cpp` — signal names already registered
- `Cmvmi.hpp` — handler declaration already present
- `MgmtSrvr.hpp` / `MgmtSrvr.cpp` — generic request sender
- `Services.hpp` / `Services.cpp` — generic API session handler
- `mgmapi.h` / `mgmapi.cpp` — generic client API function
- `ndb_mgmd_error.h` / `ndberror.cpp` — error code already defined

### Design Notes

- **Uint64 encoding**: Values are split into two Uint32 signal words (high/low). This is handled automatically by the signal infrastructure. The Cmvmi handler reconstructs with `(Uint64(high) << 32) | Uint64(low)`.
- **ALL support**: When nodeId is 0, `MgmtSrvr::set_config_param_request()` sends to all data nodes. The `executeForAll()` function in CommandInterpreter routes "ALL SET ..." as a single call with nodeId=0.
- **ndbinfo update**: Cmvmi updates ConfigValues before dispatching to the target block. The `config_values` table in ndbinfo reads from these values, so it reflects changes immediately.
- **Two-phase update**: Step 1 persists to management server config (survives restarts). Step 2 applies to running nodes immediately. If step 2 fails, the config is still saved and takes effect on next restart.

### Verification Checklist

1. Build: `cmake -DWITH_NDB=1 . && make -j$(nproc)`
2. `ndb_mgm -e "ALL SET YourParam <value>"` — succeeds
3. `ndb_mgm -e "1 SET YourParam <value>"` — succeeds for node 1
4. `SELECT config_value FROM ndbinfo.config_values WHERE config_param = <key>` — shows new value
5. Invalid value rejected with helpful error message
6. Value persists across management server restart (config saved)
