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

package org.apache.celeborn.tests.flink;

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.flink.api.common.{ExecutionMode, InputDependencyConstraint, RuntimeExecutionMode}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{Configuration, ExecutionOptions}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite;

class WordCountTest extends AnyFunSuite with Logging with MiniClusterFeature
  with BeforeAndAfterAll {

  val WORDS = Array[String](
    "To be, or not to be,--that is the question:--",
    "Whether 'tis nobler in the mind to suffer",
    "The slings and arrows of outrageous fortune",
    "Or to take arms against a sea of troubles,",
    "And by opposing end them?--To die,--to sleep,--",
    "No more; and by a sleep to say we end",
    "The heartache, and the thousand natural shocks",
    "That flesh is heir to,--'tis a consummation",
    "Devoutly to be wish'd. To die,--to sleep;--",
    "To sleep! perchance to dream:--ay, there's the rub;",
    "For in that sleep of death what dreams may come,",
    "When we have shuffled off this mortal coil,",
    "Must give us pause: there's the respect",
    "That makes calamity of so long life;",
    "For who would bear the whips and scorns of time,",
    "The oppressor's wrong, the proud man's contumely,",
    "The pangs of despis'd love, the law's delay,",
    "The insolence of office, and the spurns",
    "That patient merit of the unworthy takes,",
    "When he himself might his quietus make",
    "With a bare bodkin? who would these fardels bear,",
    "To grunt and sweat under a weary life,",
    "But that the dread of something after death,--",
    "The undiscover'd country, from whose bourn",
    "No traveller returns,--puzzles the will,",
    "And makes us rather bear those ills we have",
    "Than fly to others that we know not of?",
    "Thus conscience does make cowards of us all;",
    "And thus the native hue of resolution",
    "Is sicklied o'er with the pale cast of thought;",
    "And enterprises of great pith and moment,",
    "With this regard, their currents turn awry,",
    "And lose the name of action.--Soft you now!",
    "The fair Ophelia!--Nymph, in thy orisons",
    "Be all my sins remember'd.")

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup rss mini cluster")
    setUpMiniCluster()
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop rss mini cluster")
    shutdownMiniCluster()
  }

  test("celeborn flink integration test - word count") {
    // set up execution environment
    val configuration = new Configuration
    configuration.setString(
      "shuffle-service-factory.class",
      "org.apache.celeborn.plugin.flink.RemoteShuffleServiceFactory")
    configuration.setString("celeborn.master.endpoints", "localhost:9097")
    configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH)
    val env = ExecutionEnvironment.createLocalEnvironment(configuration)
    env.getConfig.setExecutionMode(ExecutionMode.BATCH)
    env.getConfig.setParallelism(4)
    env.getConfig.setDefaultInputDependencyConstraint(InputDependencyConstraint.ALL)
    // make parameters available in the web interface
    val text = env.fromCollection(WORDS)

    val counts = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .groupBy(0)
      .sum(1)

    println("Printing result to stdout. Use --output to specify output path.")

    counts.print()
  }

}
