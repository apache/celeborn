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

package org.apache.celeborn.rest.v1.master.invoker;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeFeature;
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;

@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class RFC3339InstantDeserializer<T extends Temporal> extends InstantDeserializer<T> {

    private final static boolean DEFAULT_NORMALIZE_ZONE_ID = JavaTimeFeature.NORMALIZE_DESERIALIZED_ZONE_ID.enabledByDefault();
    private final static boolean DEFAULT_ALWAYS_ALLOW_STRINGIFIED_DATE_TIMESTAMPS
    = JavaTimeFeature.ALWAYS_ALLOW_STRINGIFIED_DATE_TIMESTAMPS.enabledByDefault();

    public static final RFC3339InstantDeserializer<Instant> INSTANT = new RFC3339InstantDeserializer<>(
        Instant.class, DateTimeFormatter.ISO_INSTANT,
        Instant::from,
        a -> Instant.ofEpochMilli( a.value ),
        a -> Instant.ofEpochSecond( a.integer, a.fraction ),
        null,
        true, // yes, replace zero offset with Z
        DEFAULT_NORMALIZE_ZONE_ID,
        DEFAULT_ALWAYS_ALLOW_STRINGIFIED_DATE_TIMESTAMPS
    );

    public static final RFC3339InstantDeserializer<OffsetDateTime> OFFSET_DATE_TIME = new RFC3339InstantDeserializer<>(
        OffsetDateTime.class, DateTimeFormatter.ISO_OFFSET_DATE_TIME,
        OffsetDateTime::from,
        a -> OffsetDateTime.ofInstant( Instant.ofEpochMilli( a.value ), a.zoneId ),
        a -> OffsetDateTime.ofInstant( Instant.ofEpochSecond( a.integer, a.fraction ), a.zoneId ),
        (d, z) -> ( d.isEqual( OffsetDateTime.MIN ) || d.isEqual( OffsetDateTime.MAX ) ?
        d :
        d.withOffsetSameInstant( z.getRules().getOffset( d.toLocalDateTime() ) ) ),
        true, // yes, replace zero offset with Z
        DEFAULT_NORMALIZE_ZONE_ID,
        DEFAULT_ALWAYS_ALLOW_STRINGIFIED_DATE_TIMESTAMPS
    );

    public static final RFC3339InstantDeserializer<ZonedDateTime> ZONED_DATE_TIME = new RFC3339InstantDeserializer<>(
        ZonedDateTime.class, DateTimeFormatter.ISO_ZONED_DATE_TIME,
        ZonedDateTime::from,
        a -> ZonedDateTime.ofInstant( Instant.ofEpochMilli( a.value ), a.zoneId ),
        a -> ZonedDateTime.ofInstant( Instant.ofEpochSecond( a.integer, a.fraction ), a.zoneId ),
        ZonedDateTime::withZoneSameInstant,
        false, // keep zero offset and Z separate since zones explicitly supported
        DEFAULT_NORMALIZE_ZONE_ID,
        DEFAULT_ALWAYS_ALLOW_STRINGIFIED_DATE_TIMESTAMPS
    );

    protected RFC3339InstantDeserializer(
            Class<T> supportedType,
            DateTimeFormatter formatter,
            Function<TemporalAccessor, T> parsedToValue,
            Function<FromIntegerArguments, T> fromMilliseconds,
            Function<FromDecimalArguments, T> fromNanoseconds,
            BiFunction<T, ZoneId, T> adjust,
            boolean replaceZeroOffsetAsZ,
            boolean normalizeZoneId,
            boolean readNumericStringsAsTimestamp) {
        super(
                supportedType,
                formatter,
                parsedToValue,
                fromMilliseconds,
                fromNanoseconds,
                adjust,
                replaceZeroOffsetAsZ,
                normalizeZoneId,
                readNumericStringsAsTimestamp
        );
    }

    @Override
    protected T _fromString(JsonParser p, DeserializationContext ctxt, String string0) throws IOException {
        return super._fromString(p, ctxt, string0.replace( ' ', 'T' ));
    }
}