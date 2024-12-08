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

package org.apache.celeborn.service.deploy.master.tags.ql

import org.apache.celeborn.CelebornFunSuite

class TagsQLParserSuite extends CelebornFunSuite {

  private val parser = new TagsQLParser()

  test("test tags ql parser") {

    def assertValidNodes(nodes: List[Node], operator: Operator): Unit = {
      assert(nodes.size == 2)
      assert(nodes(0).key == "env")
      assert(nodes(0).operator == operator)
      assert(nodes(0).values == Set("production"))
      assert(nodes(1).key == "region")
      assert(nodes(1).operator == operator)
      assert(nodes(1).values.contains("us-east"))
      assert(nodes(1).values.contains("us-west"))
    }

    {
      val tagsExpr = "env:production region:{us-east,us-west}"
      val nodes = parser.parse(tagsExpr)
      assertValidNodes(nodes, Equals)
    }

    {
      val tagsExpr = "env:!production region:!{us-east,us-west}"
      val nodes = parser.parse(tagsExpr)
      assertValidNodes(nodes, NotEquals)
    }
  }

}
