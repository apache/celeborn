package org.apache.celeborn.util;

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;

import org.apache.celeborn.common.CelebornConf;

public class HadoopUtils {
  public static final String MR_CELEBORN_CONF = "celeborn.xml";
  public static final String MR_CELEBORN_LC_HOST = "celeborn.lifecycleManager.host";
  public static final String MR_CELEBORN_LC_PORT = "celeborn.lifecycleManager.port";
  public static final String MR_CELEBORN_APPLICATION_ID = "celeborn.applicationId";

  public static CelebornConf fromYarnConf(JobConf conf) {
    CelebornConf tmpCelebornConf = new CelebornConf();
    Iterator<Map.Entry<String, String>> proIter = conf.iterator();
    while (proIter.hasNext()) {
      Map.Entry<String, String> property = proIter.next();
      String proName = property.getKey();
      String proValue = property.getValue();
      if (proName.startsWith("mapreduce.celeborn")) {
        tmpCelebornConf.set(proName.substring("mapreduce.".length()), proValue);
      }
    }
    return tmpCelebornConf;
  }
}
