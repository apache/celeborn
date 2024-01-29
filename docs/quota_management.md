# Quota
Celeborn supports fine-grained quota management, including four indicators: `diskBytesWritten`,
`diskFileCount`, `hdfsBytesWritten` and `hdfsFileCount`.

### Default quota
We can add quota configuration for each user in the `celeborn.quota.configuration.path` file.
We support different levels of quota configuration, which can be system level, tenant level, or user level.
The priority for the configuration to take effect is as follows: user > tenant > system.
For more information, please refer to [quota.yaml.template](../conf/quota.yaml.template).