# ThreadStack
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **threadId** | **String** | The id of the thread. | [default to null] |
| **threadName** | **String** | The name of the thread. | [default to null] |
| **threadState** | **String** | The state of the thread. | [default to null] |
| **stackTrace** | **List** | The stacktrace of the thread. | [default to []] |
| **blockedByThreadId** | **Integer** | The id of the thread that the current thread is blocked by. | [optional] [default to null] |
| **blockedByLock** | **String** | The lock that the current thread is blocked by. | [optional] [default to null] |
| **holdingLocks** | **List** | The locks that the current thread is holding. | [optional] [default to []] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

