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

package org.apache.celeborn.rest.v1.worker.invoker;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

import com.fasterxml.jackson.databind.module.SimpleModule;

@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class RFC3339JavaTimeModule extends SimpleModule {

    public RFC3339JavaTimeModule() {
        super("RFC3339JavaTimeModule");

        addDeserializer(Instant.class, RFC3339InstantDeserializer.INSTANT);
        addDeserializer(OffsetDateTime.class, RFC3339InstantDeserializer.OFFSET_DATE_TIME);
        addDeserializer(ZonedDateTime.class, RFC3339InstantDeserializer.ZONED_DATE_TIME);
    }
}
