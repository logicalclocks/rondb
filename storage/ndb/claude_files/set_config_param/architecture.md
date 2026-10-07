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
  1. Dispatches to appropriate block via switch(configKey)
     (a case may reject the value with SetConfigParamRef and return)
  2. Updates ConfigValues (for ndbinfo reporting), keeping the entry's
     stored type (Uint32 or Uint64)
  3. Sends SetConfigParamConf back to MgmtSrvr
```

The signal infrastructure (`SetConfigParam`) is **already implemented** and generic — it carries a config key (Uint32) and a value (Uint64 split into two Uint32 words). Adding a new parameter only requires changes in two files.

## API Node Parameters

**Reference implementation**: `AdaptiveSendThreshold` (`CFG_API_ADAPTIVE_SEND_THRESHOLD`, 898), RONDB-1124, 2026-10-06.

The management server has no transporter to API nodes, so a parameter of the `[api]` / `[mysqld]` sections travels through the data nodes, the way ACTIVATE / DEACTIVATE / SET_HOSTNAME do:

```
MGM Client: <api id>|ALL SET <param> <value>
  1. Update every API section (ALL) or the one of <api id> (must be NODE_TYPE_API)
  2. ndb_mgm_set_config_param(handle, api id or 0, key, value)
       |
MgmtSrvr::set_config_param_request
  -> SetConfigParamReq::forApiNodes(key) -> set_api_config_param_request()
  -> SET_CONFIG_PARAM_REQ (ApiSignalLength = 5, targetNodeId) to CMVMI on every
     data node with ndbd_support_api_set_config_param(version)
       |
Data node CMVMI: forApiNodes(key) -> hands the signal to QMGR (no local
ConfigValues update; the data node has no such entry)
       |
QMGR (activate state machine, state HANDLE_SET_API_CONFIG_PARAM):
  forwards to API_CLUSTERMGR on each API node (NodeInfo::API, ZAPI_ACTIVE,
  supported version, == target unless 0), counts replies, handles API
  failures (handle_activate_failed_node), answers mgmd with
  SET_CONFIG_PARAM_CONF (ApiSignalLength = 3, apiNodesApplied) or REF
       |
API node ClusterMgr::execSET_CONFIG_PARAM_REQ: checks the sender is a data
node, applies the value (TransporterFacade), logs it when it changes,
replies CONF / REF to that data node's QMGR
```

- An API node gets one copy per data node; applying a value must be idempotent.
- mgmd: a single API node must be reached through at least one data node (sum of `apiNodesApplied` > 0), else error 5070; a non-API node id gives 5062. ALL succeeds when every data node answered.
- Version gates: older API nodes ignore unknown signals and would never answer, so QMGR sends only to `ndbd_support_api_set_config_param` versions; mgmd sends only to data nodes of that version (an older data node would take the key as one of its own).
- QMGR validates replies (state, sender node, key) and ignores unexpected ones with a log line instead of an `ndbrequire`, since API nodes send them.
- The API reads the saved value at connect (`TransporterFacade::configure`).

## Design Notes

- **Uint64 encoding**: Values are split into two Uint32 signal words (high/low). This is handled automatically by the signal infrastructure. The Cmvmi handler reconstructs with `(Uint64(high) << 32) | Uint64(low)`.
- **ALL support**: When nodeId is 0, `MgmtSrvr::set_config_param_request()` sends to all data nodes. The `executeForAll()` function in CommandInterpreter routes "ALL SET ..." as a single call with nodeId=0.
- **ndbinfo update**: Cmvmi updates ConfigValues after dispatching to the target block (so a rejected value is never recorded). The `config_values` table in ndbinfo reads from these values, so it reflects changes immediately.
- **Two-phase update**: Step 1 persists to management server config (survives restarts). Step 2 applies to running nodes immediately. If step 2 fails, the config is still saved and takes effect on next restart.
