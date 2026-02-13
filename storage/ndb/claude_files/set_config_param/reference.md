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
