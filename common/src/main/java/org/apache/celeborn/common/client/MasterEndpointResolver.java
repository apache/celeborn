package org.apache.celeborn.common.client;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.RpcNameConstants;

import java.util.Iterator;
import java.util.List;

public abstract class MasterEndpointResolver {
    private final CelebornConf conf;
    private final boolean isWorker;
    private String masterEndpointName;
    protected List<String> activeMasterEndpoints;
    protected int currentIndex;

    public MasterEndpointResolver(CelebornConf conf, boolean isWorker) {
        this.conf = conf;
        this.isWorker = isWorker;

        init();
    }

    private void init() {
        if (isWorker && conf.internalPortEnabled()) {
            // For worker, we should use the internal endpoints if internal port is enabled.
            this.masterEndpointName = RpcNameConstants.MASTER_INTERNAL_EP;
            resolve(conf.masterInternalEndpoints());
        } else {
            this.masterEndpointName = RpcNameConstants.MASTER_EP;
            resolve(conf.masterEndpoints());
        }
    }

    public String getMasterEndpointName() {
        return masterEndpointName;
    }

    public List<String> getActiveMasterEndpoints() {
        return activeMasterEndpoints;
    }

    abstract protected void resolve(String[] endpoints);

    abstract protected void update(String[] endpoints);
}
