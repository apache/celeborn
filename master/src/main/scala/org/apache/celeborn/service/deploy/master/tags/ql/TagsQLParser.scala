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

sealed trait Operator
case object Equals extends Operator
case object NotEquals extends Operator

case class Node(key: String, operator: Operator, values: Set[String])

/**
 * TagsQL uses key/value pair to give your tags more context.
 *
 * The query language supports the following syntax:
 *   - Match single value: `key:value`
 *   - Negate single value: `key:!value`
 *   - Match list of values: `key:{value1,value2}`
 *   - Negate list of values: `key:!{value1,value2}`
 *
 * Example tags expression: `env:production region:{us-east,us-west} env:!sandbox`
 * This tags expression will select all of the workers that have the following tags:
 *  - env=production
 *  - region=us-east or region=us-west
 * and will ignore all of the workers that have the following tags:
 *  - env=sandbox
 *
 * TagsQLParser defines the parsing logic for TagsQL.
 */
class TagsQLParser {

  private val SEPARATOR_TOKEN = ","
  private val NEGATION_TOKEN = "!"
  // Only allow word characters and hyphen in the key and value
  // (This includes underscore and numbers as well)
  private val VALID_CHARS = "\\w\\-"

  private val Pattern = (s"^([$VALID_CHARS]+):($NEGATION_TOKEN?)" +
    s"(?:\\{([$VALID_CHARS$SEPARATOR_TOKEN]+)}|([$VALID_CHARS]+))" + "$").r

  def parse(tagsExpr: String): List[Node] = {
    tagsExpr.split("\\s+").map(parseToken).toList
  }

  private def parseToken(token: String): Node = {
    token match {
      case Pattern(key, condition, values, value) =>
        val valuesStr = if (values != null) values else value
        val operator = condition match {
          case NEGATION_TOKEN => NotEquals
          case _ => Equals
        }
        Node(key, operator, valuesStr.split(",").toSet)

      case _ => throw new IllegalArgumentException(
          s"Found invalid token: $token while parsing the tagsExpr.")
    }
  }
}
