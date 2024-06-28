# WorkerInfo
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **host** | **String** | The host of the worker. | [default to null] |
| **rpcPort** | **Integer** | The rpc port of the worker. | [default to null] |
| **pushPort** | **Integer** | The push port of the worker. | [default to null] |
| **fetchPort** | **Integer** | The fetch port of the worker. | [default to null] |
| **replicatePort** | **Integer** | The replicate port of the worker. | [default to null] |
| **internalPort** | **Integer** | The internal port of the worker. | [default to null] |
| **slotUsed** | **Integer** | The slot used of the worker. | [optional] [default to null] |
| **lastHeartbeat** | **Long** | The last heartbeat timestamp of the worker. | [optional] [default to null] |
| **heartbeatElapsedSeconds** | **Long** | The elapsed seconds since the last heartbeat of the worker. | [optional] [default to null] |
| **diskInfos** | **Map** | a map of disk name and disk info. | [optional] [default to null] |
| **resourceConsumption** | **Map** | a map of identifier and resource consumption. | [optional] [default to null] |
| **workerRef** | **String** | The reference of the worker. | [optional] [default to null] |
| **workerState** | **String** | The state of the worker. | [optional] [default to null] |
| **workerStateStartTime** | **Long** | The start time of the worker state. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

