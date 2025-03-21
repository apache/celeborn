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

package org.apache.celeborn.verifier.plan

import scala.util.Random

import com.alibaba.fastjson2.JSONObject

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.verifier.plan.exception.PlanInvalidException

class Trigger(val policy: String, val interval: Interval, val repeat: Int) extends Serializable
  with Logging {

  val random: Boolean = policy == "random"
  val sequence: Boolean = policy == "sequence"
}

object Trigger {

  def fromJson(obj: JSONObject): Trigger = {
    val intervalObj = obj.getJSONObject("interval")
    if (intervalObj == null) {
      throw new PlanInvalidException("Trigger configuration invalid.")
    }
    val interval = Option(intervalObj.get("type")).get match {
      case "fix" => new FixInterval(Utils.timeStringAsMs(
          Option(intervalObj.getString("value")).getOrElse("5s")))
      case "range" =>
        new RangeInterval(
          Utils.timeStringAsSeconds(Option(
            intervalObj.getString("start")).getOrElse("5s")),
          Utils.timeStringAsSeconds(Option(
            intervalObj.getString("end")).getOrElse("10s")))
    }
    new Trigger(
      Option(obj.getString("policy")).getOrElse("random"),
      interval,
      Option(obj.getIntValue("repeat")).getOrElse(1))
  }
}

trait Interval extends Serializable {

  def getInterval: Long
}

class FixInterval(val interval: Long) extends Interval {

  override def getInterval: Long = {
    interval
  }
}

class RangeInterval(start: Long, end: Long) extends Interval {

  override def getInterval: Long = {
    (start + Random.nextInt(end.intValue() - start.intValue())) * 1000
  }
}
