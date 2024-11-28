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

package org.apache.celeborn.client

object ClientUtils {
  /**
   * Check if all the mapper attempts are finished. If any of the attempts is not finished, return false.
   * This method checks the attempts array in reverse order, which can be faster if the unfinished attempts
   * are more likely to be towards the end of the array.
   *
   * @param attempts The mapper attempts.
   * @return True if all mapper attempts are finished, false otherwise.
   */
  def areAllMapperAttemptsFinished(attempts: Array[Int]): Boolean = {
    var i = attempts.length - 1
    while (i >= 0) {
      if (attempts(i) < 0) {
        return false
      }
      i -= 1
    }
    true
  }
}
