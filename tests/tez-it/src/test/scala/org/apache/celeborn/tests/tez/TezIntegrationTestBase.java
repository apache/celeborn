package org.apache.celeborn.tests.tez;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.tez.plugin.util.CelebornTezUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.CelebornDagAppMaster;
import org.apache.tez.test.MiniTezCluster;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezIntegrationTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TezIntegrationTestBase.class);
    protected static MiniTezCluster miniTezCluster;

    @Before
    public void beforeClass() throws Exception {
        miniTezCluster = new MiniTezCluster(TezIntegrationTestBase.class.getName(), 1, 1, 1);
        Configuration conf = new Configuration();
        miniTezCluster.init(conf);
        miniTezCluster.start();
    }

    public void run() throws Exception {


        //TezConfiguration appConf = new TezConfiguration(miniTezCluster.getConfig());
        TezConfiguration appConf = new TezConfiguration();
        //updateCommonConfiguration(appConf);
        //runTezApp(appConf, getTestTool(), getTestArgs("test"));

        appConf = new TezConfiguration();
        updateRssConfiguration(appConf);
        runTezApp(appConf, getTestTool(), getTestArgs("rss"));


    }

    protected void runTezApp(TezConfiguration tezConf, Tool tool, String[] args) throws Exception {
        ToolRunner.run(tezConf, tool, args);
    }

    public void updateCommonConfiguration(Configuration appConf) throws Exception {
        // appConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());
        appConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 512);
        appConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx384m");
        appConf.setInt(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB, 512);
        appConf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, " -Xmx384m");
    }

    public void updateRssConfiguration(Configuration appConf) throws Exception {
        // appConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());
        appConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 512);
        appConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx384m");
        appConf.set(TezConfiguration.TEZ_PREFIX + "appmaster.class.name", "org.apache.tez.dag.app.CelebornDagAppMaster");
        appConf.setInt(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB, 512);
        appConf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, " -Xmx384m");
        appConf.set(CelebornTezUtils.TEZ_PREFIX + CelebornConf.MASTER_ENDPOINTS().key(), "localhost:9097");
        appConf.set(
                TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS,
                TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS_DEFAULT + " -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 " + CelebornDagAppMaster.class.getName());
    }

    public Tool getTestTool() {
        return null;
    }

    public String[] getTestArgs(String uniqueOutputName) {
        return new String[0];
    }

    public String getOutputDir(String uniqueOutputName) {
        return null;
    }
}
