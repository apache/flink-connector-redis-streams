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

package org.apache.flink.connector.redis.streams.source.split;

import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link RedisStreamsSourceSplit}, {@link RedisStreamsSourceSplitState} and the
 * serializer.
 */
class RedisStreamsSourceSplitTest {

    @Test
    void identityIsStreamKeyOnly() {
        RedisStreamsSourceSplit a = new RedisStreamsSourceSplit("s", "1-0", "9-0");
        RedisStreamsSourceSplit b = new RedisStreamsSourceSplit("s", "2-0", null);
        RedisStreamsSourceSplit c = new RedisStreamsSourceSplit("other", "1-0", "9-0");

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b).isNotEqualTo(c);
    }

    @Test
    void splitStateAdvancesAndSnapshotsBackIntoSplit() {
        RedisStreamsSourceSplit base = new RedisStreamsSourceSplit("s", "10-0", "100-0");
        RedisStreamsSourceSplitState state = new RedisStreamsSourceSplitState(base);

        assertThat(state.getCurrentEntryId()).isEqualTo("10-0");

        state.setCurrentEntryId("42-3");
        RedisStreamsSourceSplit snapshot = state.toSplit();
        assertThat(snapshot.getStreamKey()).isEqualTo("s");
        assertThat(snapshot.getStartingEntryId()).isEqualTo("42-3");
        assertThat(snapshot.getStoppingEntryId()).isEqualTo("100-0");
    }

    @Test
    void rejectsNullStreamKey() {
        assertThatThrownBy(() -> new RedisStreamsSourceSplit(null, "1-0", null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new RedisStreamsSourceSplitState(null))
                .isInstanceOf(NullPointerException.class);
    }

    static Stream<Arguments> serializationCases() {
        return Stream.of(
                Arguments.of("s", null, null),
                Arguments.of("s", "1-0", null),
                Arguments.of("s", null, "9-0"),
                Arguments.of("s", "1-0", "9-0"),
                Arguments.of("stream-with-special-chars:42", "0-0", "9999999999-99"));
    }

    @ParameterizedTest
    @MethodSource("serializationCases")
    void roundTripV1(String key, String start, String stop) throws IOException {
        RedisStreamsSourceSplitSerializer serializer = RedisStreamsSourceSplitSerializer.INSTANCE;
        RedisStreamsSourceSplit original = new RedisStreamsSourceSplit(key, start, stop);

        byte[] bytes = serializer.serialize(original);
        RedisStreamsSourceSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(restored.getStreamKey()).isEqualTo(key);
        assertThat(restored.getStartingEntryId()).isEqualTo(start);
        assertThat(restored.getStoppingEntryId()).isEqualTo(stop);
    }

    @Test
    void unknownVersionRejected() {
        // Any version other than CURRENT_VERSION (1) must be rejected cleanly.
        assertThatThrownBy(
                        () ->
                                RedisStreamsSourceSplitSerializer.INSTANCE.deserialize(
                                        99, new byte[] {1, 2, 3}))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported");
    }
}
