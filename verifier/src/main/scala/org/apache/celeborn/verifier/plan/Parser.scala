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

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import com.alibaba.fastjson2.{JSON, JSONObject}

import org.apache.celeborn.verifier.action.{Action, ActionIdentity}
import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.plan.exception.PlanInvalidException

object Parser {

  def parse(planStr: String, conf: VerifierConf): VerificationPlan = {
    val planJsonObj = JSON.parseObject(planStr)
    val actions = planJsonObj.getJSONArray("actions").asScala.map(obj => {
      val actionJsonObj = obj.asInstanceOf[JSONObject]
      Action.parseJson(actionJsonObj, conf)
    }).toList
    val trigger = Trigger.fromJson(planJsonObj.getJSONObject("trigger"))
    if (trigger.random
      && actions.map(_.identity())
        .exists(_.equalsIgnoreCase(ActionIdentity.ACTION_DISK_CORRUPT_META))) {
      throw new PlanInvalidException(
        s"Random trigger does not support ${ActionIdentity.ACTION_DISK_CORRUPT_META} action.")
    }
    val checker = Checker.getChecker(Option(planJsonObj.getString("checker")))
    new VerificationPlan(actions, trigger, checker)
  }
}
