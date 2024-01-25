# Quota
Celeborn supports fine-grained quota management, including four indicators: `diskBytesWritten`,
`diskFileCount`, `hdfsBytesWritten` and `hdfsFileCount`.

### Default quota
We can add quota configuration for each user in the `celeborn.quota.configuration.path` file.
It is worth noting that when we add a configuration with the values of tenantId and name as default,
it will take effect for all users who have not been configured.
For more information, please refer to [quota.yaml.template](../conf/quota.yaml.template).