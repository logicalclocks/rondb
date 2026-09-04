# SET Config Param — Reference

## Key File Locations

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

## Runtime-Settable Parameters (MGM client `SET`)

| Parameter | Key | Type | Runtime dispatch in the data node |
|-----------|-----|------|-----------------------------------|
| `MaxDiskWriteSpeed` | `CFG_DB_MAX_DISK_WRITE_SPEED` (639) | CI_INT64 | `SET_CONFIG_PARAM_REQ` -> Cmvmi -> `DUMP BackupMaxWriteSpeed64` to BACKUP |
| `EnableProactiveDeadlockDetection` | `CFG_DB_ENABLE_PROACTIVE_DEADLOCK_DETECTION` (708) | CI_BOOL | client sends `DUMP DumpStateOrd::DeadlockDetection` per node (DBTC/DBACC) |
| `RdmaLogLevel` | `CFG_RDMA_LOG_LEVEL` (533) | CI_INT | client sends `DUMP 103020` (`CmvmiSetRdmaLogLevel`) per node |
| `CompiledInterpreter` | `CFG_DB_COMPILED_INTERPRETER` (709) | CI_ENUM `OFF`/`AUTO`/`ON` = 0/1/2 | `SET_CONFIG_PARAM_REQ` -> Cmvmi -> `dbtup_jit_set_mode()` (RONDB-1056 JIT mode word, consulted at every compile decision; already compiled programs stay cached) |

`ndb_mgm -e "ALL SET CompiledInterpreter OFF"` / `"1 SET CompiledInterpreter on"` / `"ALL SET CompiledInterpreter 2"` — enum names are case-insensitive, numbers accepted. MTR: `mysql-test/suite/ndb/t/ndb_set_compiled_interpreter.test` (the others: `ndb_config_set.test`).

## Files That Do NOT Need Changes for New Parameters

These files contain the generic signal infrastructure and are already complete:

- `SetConfigParam.hpp` — signal definition (generic key + Uint64 value)
- `GlobalSignalNumbers.h` — GSN numbers already allocated
- `SignalNames.cpp` — signal names already registered
- `Cmvmi.hpp` — handler declaration already present
- `MgmtSrvr.hpp` / `MgmtSrvr.cpp` — generic request sender
- `Services.hpp` / `Services.cpp` — generic API session handler
- `mgmapi.h` / `mgmapi.cpp` — generic client API function
- `ndb_mgmd_error.h` / `ndberror.cpp` — error code already defined

## Verification Checklist

1. Build: `cmake -DWITH_NDB=1 . && make -j$(nproc)`
2. `ndb_mgm -e "ALL SET YourParam <value>"` — succeeds
3. `ndb_mgm -e "1 SET YourParam <value>"` — succeeds for node 1
4. `SELECT config_value FROM ndbinfo.config_values WHERE config_param = <key>` — shows new value
5. Invalid value rejected with helpful error message
6. Value persists across management server restart (config saved)
