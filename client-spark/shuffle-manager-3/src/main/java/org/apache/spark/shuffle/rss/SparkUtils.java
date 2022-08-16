/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.rss;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.LongAdder;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.sql.execution.UnsafeRowSerializer;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.storage.BlockManagerId;

import com.aliyun.emr.rss.common.RssConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkUtils {
  private static final Logger logger = LoggerFactory.getLogger(SparkUtils.class);
  public static MapStatus createMapStatus(
      BlockManagerId loc, long[] uncompressedSizes, long mapTaskId) {
    return MapStatus$.MODULE$.apply(loc, uncompressedSizes, mapTaskId);
  }

  public static SQLMetric getUnsafeRowSerializerDataSizeMetric(UnsafeRowSerializer serializer) {
    try {
      Field field = serializer.getClass().getDeclaredField("dataSize");
      field.setAccessible(true);
      return (SQLMetric) field.get(serializer);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      logger.warn("Failed to get dataSize metric, aqe won`t work properly.");
    }
    return null;
  }

  public static long[] unwrap(LongAdder[] adders) {
    int adderCounter = adders.length;
    long[] res = new long[adderCounter];
    for (int i = 0; i < adderCounter; i++) {
      res[i] = adders[i].longValue();
    }
    return res;
  }

  /**
   * make rss conf from spark conf
   */
  public static RssConf fromSparkConf(SparkConf conf) {
    RssConf tmpRssConf = new RssConf();
    for (Tuple2<String, String> kv : conf.getAll()) {
      if (kv._1.startsWith("spark.rss.")) {
        tmpRssConf.set(kv._1.substring("spark.".length()), kv._2);
      }
    }
    return tmpRssConf;
  }

  public static String genNewAppId(SparkContext context) {
    return context.applicationAttemptId()
            .map(id -> context.applicationId() + "_" + id)
            .getOrElse(context::applicationId);
  }
}
