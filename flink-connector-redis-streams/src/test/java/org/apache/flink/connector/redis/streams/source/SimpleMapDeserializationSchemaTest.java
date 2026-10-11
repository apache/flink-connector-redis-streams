/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link SimpleMapDeserializationSchema}. */
class SimpleMapDeserializationSchemaTest {

    @Test
    void deserializePassesThroughFieldMap() throws Exception {
        SimpleMapDeserializationSchema schema = new SimpleMapDeserializationSchema();
        Map<String, String> fields = Map.of("user", "alice", "action", "login");

        Map<String, String> result = schema.deserialize("stream", "1-0", fields);

        assertThat(result).isEqualTo(fields);
    }

    @Test
    void deserializeEmptyFieldsReturnsEmptyMap() throws Exception {
        SimpleMapDeserializationSchema schema = new SimpleMapDeserializationSchema();
        Map<String, String> result = schema.deserialize("stream", "1-0", Map.of());
        assertThat(result).isEmpty();
    }

    @Test
    void getProducedTypeIsMapStringString() {
        SimpleMapDeserializationSchema schema = new SimpleMapDeserializationSchema();
        TypeInformation<Map<String, String>> expected =
                TypeInformation.of(new TypeHint<Map<String, String>>() {});
        assertThat(schema.getProducedType()).isEqualTo(expected);
    }

    @Test
    void schemaIsSerializable() throws Exception {
        SimpleMapDeserializationSchema schema = new SimpleMapDeserializationSchema();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(schema);
        }
        try (ObjectInputStream ois =
                new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
            SimpleMapDeserializationSchema restored =
                    (SimpleMapDeserializationSchema) ois.readObject();
            Map<String, String> fields = Map.of("k", "v");
            assertThat(restored.deserialize("s", "1-0", fields)).isEqualTo(fields);
        }
    }
}
