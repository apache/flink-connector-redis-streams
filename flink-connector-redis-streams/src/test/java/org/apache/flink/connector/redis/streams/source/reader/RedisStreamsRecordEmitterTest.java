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

package org.apache.flink.connector.redis.streams.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.redis.streams.source.RedisStreamsDeserializationSchema;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplit;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplitState;

import io.lettuce.core.StreamMessage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link RedisStreamsRecordEmitter}. */
class RedisStreamsRecordEmitterTest {

    /** Simple capturing source output. */
    private static final class CapturingOutput<T> implements SourceOutput<T> {
        final List<T> records = new ArrayList<>();

        @Override
        public void collect(T record) {
            records.add(record);
        }

        @Override
        public void collect(T record, long timestamp) {
            records.add(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }

    private static final class FieldDeserializer
            implements RedisStreamsDeserializationSchema<String> {
        @Override
        public String deserialize(String streamKey, String entryId, Map<String, String> fields) {
            // Returning null acts as a filter (e.g. when a required field is missing).
            return fields.get("payload");
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    private static final class ThrowingDeserializer
            implements RedisStreamsDeserializationSchema<String> {
        @Override
        public String deserialize(String streamKey, String entryId, Map<String, String> fields) {
            throw new IllegalStateException("boom");
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    @Test
    void emitsNonNullRecordsAndAdvancesEntryId() throws Exception {
        RedisStreamsRecordEmitter<String> emitter =
                new RedisStreamsRecordEmitter<>(new FieldDeserializer());
        CapturingOutput<String> out = new CapturingOutput<>();
        RedisStreamsSourceSplitState state =
                new RedisStreamsSourceSplitState(new RedisStreamsSourceSplit("s"));

        emitter.emitRecord(message("s", "1-0", Map.of("payload", "first")), out, state);
        emitter.emitRecord(message("s", "2-0", Map.of("payload", "second")), out, state);

        assertThat(out.records).containsExactly("first", "second");
        assertThat(state.getCurrentEntryId()).isEqualTo("2-0");
    }

    @Test
    void filteredRecordsAdvanceEntryIdToAvoidUnboundedPelGrowth() throws Exception {
        RedisStreamsRecordEmitter<String> emitter =
                new RedisStreamsRecordEmitter<>(new FieldDeserializer());
        CapturingOutput<String> out = new CapturingOutput<>();
        RedisStreamsSourceSplitState state =
                new RedisStreamsSourceSplitState(new RedisStreamsSourceSplit("s"));

        // No "payload" key → schema returns null → record filtered out, but the entry id MUST
        // still advance so the message is XACKed at next checkpoint and not re-delivered
        // forever via PEL recovery.
        emitter.emitRecord(message("s", "1-0", Map.of("other", "x")), out, state);

        assertThat(out.records).isEmpty();
        assertThat(state.getCurrentEntryId()).isEqualTo("1-0");
    }

    @Test
    void deserializationFailureSurfacesAsException() {
        RedisStreamsRecordEmitter<String> emitter =
                new RedisStreamsRecordEmitter<>(new ThrowingDeserializer());
        CapturingOutput<String> out = new CapturingOutput<>();
        RedisStreamsSourceSplitState state =
                new RedisStreamsSourceSplitState(new RedisStreamsSourceSplit("s"));

        assertThatThrownBy(() -> emitter.emitRecord(message("s", "1-0", Map.of()), out, state))
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to deserialize record from stream s id 1-0");
        // Failed records do NOT advance the entry id — they remain in PEL for retry.
        assertThat(state.getCurrentEntryId()).isNull();
    }

    private static StreamMessage<String, String> message(
            String stream, String id, Map<String, String> body) {
        return new StreamMessage<>(stream, id, body);
    }
}
