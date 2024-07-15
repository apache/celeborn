package org.apache.celeborn.common.client;

import org.apache.celeborn.common.CelebornConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

public class StaticMasterEndpointResolver extends MasterEndpointResolver {
    private static final Logger LOG = LoggerFactory.getLogger(StaticMasterEndpointResolver.class);

    public StaticMasterEndpointResolver(CelebornConf conf, boolean isWorker) {
        super(conf, isWorker);
    }

    @Override
    protected void resolve(String[] endpoints) {
        this.activeMasterEndpoints = Arrays.asList(endpoints);
        this.currentIndex = 0;

        Collections.shuffle(this.activeMasterEndpoints);
        LOG.info("masterEndpoints = {}", activeMasterEndpoints);
    }

    @Override
    protected void update(String[] endpoints) {}
}
