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

package org.apache.flink.connector.redis.streams.source.enumerator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link RedisStreamsSourceEnumeratorState} and its serializer. */
class RedisStreamsSourceEnumeratorStateTest {

    static Stream<Set<String>> roundTripCases() {
        return Stream.of(
                Collections.emptySet(),
                Set.of("only-one"),
                Set.of("a", "b", "c", "with:special-chars:42"));
    }

    @ParameterizedTest
    @MethodSource("roundTripCases")
    void serializationRoundTripV3PendingOnly(Set<String> pending) throws IOException {
        RedisStreamsSourceEnumeratorState state = new RedisStreamsSourceEnumeratorState(pending);
        byte[] bytes = RedisStreamsSourceEnumeratorStateSerializer.INSTANCE.serialize(state);
        RedisStreamsSourceEnumeratorState restored =
                RedisStreamsSourceEnumeratorStateSerializer.INSTANCE.deserialize(
                        RedisStreamsSourceEnumeratorStateSerializer.INSTANCE.getVersion(), bytes);

        assertThat(restored.getPendingSplits()).isEqualTo(pending);
        assertThat(restored.getStoppingEntryIds()).isEmpty();
    }

    @Test
    void serializationRoundTripV3PreservesStoppingEntryIds() throws IOException {
        Set<String> pending = Set.of("stream-a");
        Map<String, String> stoppingIds =
                Map.of("stream-a", "100-0", "stream-b", "9999-7", "stream-c", "0-0");
        RedisStreamsSourceEnumeratorState state =
                new RedisStreamsSourceEnumeratorState(new HashSet<>(pending), stoppingIds);

        byte[] bytes = RedisStreamsSourceEnumeratorStateSerializer.INSTANCE.serialize(state);
        RedisStreamsSourceEnumeratorState restored =
                RedisStreamsSourceEnumeratorStateSerializer.INSTANCE.deserialize(
                        RedisStreamsSourceEnumeratorStateSerializer.INSTANCE.getVersion(), bytes);

        assertThat(restored.getPendingSplits()).containsExactlyElementsOf(pending);
        assertThat(restored.getStoppingEntryIds()).isEqualTo(stoppingIds);
    }

    @Test
    void unknownVersionRejected() {
        // Any version other than CURRENT_VERSION (1) must be rejected cleanly.
        assertThatThrownBy(
                        () ->
                                RedisStreamsSourceEnumeratorStateSerializer.INSTANCE.deserialize(
                                        99, new byte[] {0, 0, 0, 0}))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported");
    }

    @Test
    void stateIsImmutableAfterConstruction() {
        // Mutations of the input collections must not affect the stored state.
        Set<String> mutablePending = new HashSet<>(Set.of("stream-a"));
        Map<String, String> mutableStopping = new java.util.HashMap<>(Map.of("stream-a", "100-0"));
        RedisStreamsSourceEnumeratorState state =
                new RedisStreamsSourceEnumeratorState(mutablePending, mutableStopping);

        mutablePending.add("intruder");
        mutableStopping.put("intruder", "0-0");

        assertThat(state.getPendingSplits()).doesNotContain("intruder");
        assertThat(state.getStoppingEntryIds()).doesNotContainKey("intruder");
    }

    @Test
    void nullValueInStoppingEntryIdsRejected() {
        Map<String, String> withNull = new java.util.HashMap<>();
        withNull.put("stream-a", null);
        assertThatThrownBy(
                        () ->
                                new RedisStreamsSourceEnumeratorState(
                                        Set.of("stream-a"), withNull))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null values");
    }

    @Test
    void equalsAndHashCodeReflectPendingSet() {
        Set<String> a = new HashSet<>(Set.of("x", "y"));
        Set<String> b = new HashSet<>(Set.of("y", "x"));
        assertThat(new RedisStreamsSourceEnumeratorState(a))
                .isEqualTo(new RedisStreamsSourceEnumeratorState(b))
                .hasSameHashCodeAs(new RedisStreamsSourceEnumeratorState(b));
    }

    @Test
    void rejectsNullPending() {
        assertThatThrownBy(() -> new RedisStreamsSourceEnumeratorState(null))
                .isInstanceOf(NullPointerException.class);
    }
}
