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

package org.apache.spark.shuffle.celeborn;

import java.io.IOException;

import org.apache.spark.ShuffleDependency;
import org.apache.spark.sql.types.StructType;

import org.apache.celeborn.reflect.DynFields;

public class CustomShuffleDependencyUtils {

  /**
   * Columnar Shuffle requires a field, `ShuffleDependency#schema`, which does not exist in vanilla
   * Spark.
   */
  private static final DynFields.UnboundField<StructType> SCHEMA_FIELD =
      DynFields.builder().hiddenImpl(ShuffleDependency.class, "schema").defaultAlwaysNull().build();

  public static StructType getSchema(ShuffleDependency<?, ?, ?> dep) throws IOException {
    StructType schema = SCHEMA_FIELD.bind(dep).get();
    if (schema == null) {
      throw new IOException("Failed to get Schema, columnar shuffle won't work properly.");
    }
    return schema;
  }
}
