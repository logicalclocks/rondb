# SET Config Param — Architecture

**Reference implementation**: `MaxDiskWriteSpeed` (RONDB-1017).

## Signal Flow

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

## Design Notes

- **Uint64 encoding**: Values are split into two Uint32 signal words (high/low). This is handled automatically by the signal infrastructure. The Cmvmi handler reconstructs with `(Uint64(high) << 32) | Uint64(low)`.
- **ALL support**: When nodeId is 0, `MgmtSrvr::set_config_param_request()` sends to all data nodes. The `executeForAll()` function in CommandInterpreter routes "ALL SET ..." as a single call with nodeId=0.
- **ndbinfo update**: Cmvmi updates ConfigValues before dispatching to the target block. The `config_values` table in ndbinfo reads from these values, so it reflects changes immediately.
- **Two-phase update**: Step 1 persists to management server config (survives restarts). Step 2 applies to running nodes immediately. If step 2 fails, the config is still saved and takes effect on next restart.
