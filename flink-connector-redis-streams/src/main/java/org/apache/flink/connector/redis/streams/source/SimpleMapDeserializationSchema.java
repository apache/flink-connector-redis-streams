/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.redis.streams.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Map;

/**
 * A simple deserialization schema that passes through Redis Stream entries as Map<String, String>.
 */
@PublicEvolving
public class SimpleMapDeserializationSchema
        implements RedisStreamsDeserializationSchema<Map<String, String>> {

    private static final long serialVersionUID = 1L;

    @Override
    public Map<String, String> deserialize(
            String streamKey, String entryId, Map<String, String> fields) {
        return fields;
    }

    @Override
    public TypeInformation<Map<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Map<String, String>>() {});
    }
}
