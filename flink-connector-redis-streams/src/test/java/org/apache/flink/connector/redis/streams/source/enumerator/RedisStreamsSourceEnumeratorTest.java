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

import org.apache.flink.connector.redis.streams.source.config.RedisStreamsSourceConfig;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplit;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RedisStreamsSourceEnumerator}. */
class RedisStreamsSourceEnumeratorTest {

    private static RedisStreamsSourceConfig cfg(boolean bounded, String... keys) {
        return RedisStreamsSourceConfig.builder()
                .setStreamKeys(List.of(keys))
                .setBounded(bounded)
                .build();
    }

    private static List<RedisStreamsSourceSplit> assignedSplits(
            TestingSplitEnumeratorContext<RedisStreamsSourceSplit> ctx, int reader) {
        TestingSplitEnumeratorContext.SplitAssignmentState<RedisStreamsSourceSplit> state =
                ctx.getSplitAssignments().get(reader);
        return state == null ? List.of() : new ArrayList<>(state.getAssignedSplits());
    }

    @Test
    void unboundedAssignsRoundRobinAndDoesNotSignalNoMoreSplits() {
        TestingSplitEnumeratorContext<RedisStreamsSourceSplit> ctx =
                new TestingSplitEnumeratorContext<>(2);
        ctx.registerReader(0, "host-0");
        ctx.registerReader(1, "host-1");

        RedisStreamsSourceEnumerator enumerator =
                new RedisStreamsSourceEnumerator(
                        cfg(false, "a", "b", "c"), ctx, null, never -> "0-0");
        enumerator.start();
        enumerator.addReader(0);
        enumerator.addReader(1);

        Set<String> r0 =
                assignedSplits(ctx, 0).stream()
                        .map(RedisStreamsSourceSplit::splitId)
                        .collect(Collectors.toSet());
        Set<String> r1 =
                assignedSplits(ctx, 1).stream()
                        .map(RedisStreamsSourceSplit::splitId)
                        .collect(Collectors.toSet());

        Set<String> union = new HashSet<>();
        union.addAll(r0);
        union.addAll(r1);
        assertThat(union).containsExactlyInAnyOrder("a", "b", "c");
        Set<String> intersection = new HashSet<>(r0);
        intersection.retainAll(r1);
        assertThat(intersection).as("no split is assigned to two readers").isEmpty();

        assertThat(ctx.getSplitAssignments().get(0).hasReceivedNoMoreSplitsSignal()).isFalse();
        assertThat(ctx.getSplitAssignments().get(1).hasReceivedNoMoreSplitsSignal()).isFalse();
        for (RedisStreamsSourceSplit s : assignedSplits(ctx, 0)) {
            assertThat(s.getStoppingEntryId()).isNull();
        }
        for (RedisStreamsSourceSplit s : assignedSplits(ctx, 1)) {
            assertThat(s.getStoppingEntryId()).isNull();
        }
    }

    @Test
    void boundedFreezesStoppingIdsAtStartAndSignalsNoMoreSplits() {
        TestingSplitEnumeratorContext<RedisStreamsSourceSplit> ctx =
                new TestingSplitEnumeratorContext<>(1);
        ctx.registerReader(0, "host-0");

        Function<String, String> lookup = Map.of("a", "100-0", "b", "200-7")::get;
        RedisStreamsSourceEnumerator enumerator =
                new RedisStreamsSourceEnumerator(cfg(true, "a", "b"), ctx, null, lookup);
        enumerator.start();
        enumerator.addReader(0);

        List<RedisStreamsSourceSplit> assigned = assignedSplits(ctx, 0);
        assertThat(assigned)
                .extracting(RedisStreamsSourceSplit::splitId)
                .containsExactlyInAnyOrder("a", "b");
        for (RedisStreamsSourceSplit split : assigned) {
            assertThat(split.getStoppingEntryId())
                    .isEqualTo(split.splitId().equals("a") ? "100-0" : "200-7");
        }
        assertThat(ctx.getSplitAssignments().get(0).hasReceivedNoMoreSplitsSignal()).isTrue();
    }

    @Test
    void addSplitsBackPlacesSplitsBackIntoPending() {
        TestingSplitEnumeratorContext<RedisStreamsSourceSplit> ctx =
                new TestingSplitEnumeratorContext<>(2);
        ctx.registerReader(0, "host-0");
        ctx.registerReader(1, "host-1");

        RedisStreamsSourceEnumerator enumerator =
                new RedisStreamsSourceEnumerator(cfg(false, "a", "b"), ctx, null, never -> "0-0");
        enumerator.start();
        enumerator.addReader(0);
        enumerator.addReader(1);

        List<RedisStreamsSourceSplit> reader0Splits = assignedSplits(ctx, 0);
        assertThat(reader0Splits).isNotEmpty();

        // Snapshot before addSplitsBack: reader 0's splits are NOT in the pending set
        // (they're already assigned).
        assertThat(enumerator.snapshotState(1L).getPendingSplits())
                .doesNotContainAnyElementsOf(
                        reader0Splits.stream()
                                .map(RedisStreamsSourceSplit::splitId)
                                .collect(Collectors.toList()));

        enumerator.addSplitsBack(reader0Splits, 0);

        // After re-pooling but before dispatch consumes them, snapshot includes them as pending
        // OR the dispatch placed them somewhere — either way, the union of assigned-everywhere
        // and pending-in-snapshot must still cover the original keys.
        Set<String> assignedUnion = new HashSet<>();
        assignedUnion.addAll(
                assignedSplits(ctx, 0).stream()
                        .map(RedisStreamsSourceSplit::splitId)
                        .collect(Collectors.toSet()));
        assignedUnion.addAll(
                assignedSplits(ctx, 1).stream()
                        .map(RedisStreamsSourceSplit::splitId)
                        .collect(Collectors.toSet()));
        Set<String> totalCoverage = new HashSet<>(assignedUnion);
        totalCoverage.addAll(enumerator.snapshotState(2L).getPendingSplits());

        assertThat(totalCoverage)
                .as("returned splits remain accounted for (assigned or pending)")
                .containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void restoredStateReplacesInitialPending() {
        TestingSplitEnumeratorContext<RedisStreamsSourceSplit> ctx =
                new TestingSplitEnumeratorContext<>(1);
        ctx.registerReader(0, "host-0");

        RedisStreamsSourceEnumeratorState restored =
                new RedisStreamsSourceEnumeratorState(Set.of("only-pending"));
        RedisStreamsSourceEnumerator enumerator =
                new RedisStreamsSourceEnumerator(
                        cfg(false, "a", "b", "c"), ctx, restored, never -> "0-0");
        enumerator.start();
        enumerator.addReader(0);

        assertThat(assignedSplits(ctx, 0))
                .extracting(RedisStreamsSourceSplit::splitId)
                .containsExactly("only-pending");
    }

    @Test
    void boundedRestoreUsesCheckpointedStoppingIdsInsteadOfReQueryingXinfo() {
        TestingSplitEnumeratorContext<RedisStreamsSourceSplit> ctx =
                new TestingSplitEnumeratorContext<>(1);
        ctx.registerReader(0, "host-0");

        RedisStreamsSourceEnumeratorState restored =
                new RedisStreamsSourceEnumeratorState(
                        new HashSet<>(Set.of("a")), Map.of("a", "100-0"));

        java.util.concurrent.atomic.AtomicInteger lookupCalls =
                new java.util.concurrent.atomic.AtomicInteger(0);
        Function<String, String> shiftedLookup =
                key -> {
                    lookupCalls.incrementAndGet();
                    return "9999-0";
                };

        RedisStreamsSourceEnumerator enumerator =
                new RedisStreamsSourceEnumerator(cfg(true, "a"), ctx, restored, shiftedLookup);
        enumerator.start();
        enumerator.addReader(0);

        List<RedisStreamsSourceSplit> assigned = assignedSplits(ctx, 0);
        assertThat(assigned).hasSize(1);
        assertThat(assigned.get(0).getStoppingEntryId()).isEqualTo("100-0");
        assertThat(lookupCalls.get()).isZero();
        assertThat(enumerator.snapshotState(7L).getStoppingEntryIds())
                .containsEntry("a", "100-0");
    }

    @Test
    void boundedRestoreLooksUpStoppingIdForKeysMissingFromRestoredMap() {
        TestingSplitEnumeratorContext<RedisStreamsSourceSplit> ctx =
                new TestingSplitEnumeratorContext<>(1);
        ctx.registerReader(0, "host-0");

        RedisStreamsSourceEnumeratorState legacy =
                new RedisStreamsSourceEnumeratorState(new HashSet<>(Set.of("a")));

        RedisStreamsSourceEnumerator enumerator =
                new RedisStreamsSourceEnumerator(
                        cfg(true, "a"), ctx, legacy, key -> "42-0");
        enumerator.start();
        enumerator.addReader(0);

        List<RedisStreamsSourceSplit> assigned = assignedSplits(ctx, 0);
        assertThat(assigned).hasSize(1);
        assertThat(assigned.get(0).getStoppingEntryId()).isEqualTo("42-0");
        assertThat(enumerator.snapshotState(1L).getStoppingEntryIds())
                .containsEntry("a", "42-0");
    }

    @Test
    void extractLastGeneratedIdParsesAlternatingList() {
        List<Object> info =
                List.of("length", 3L, "last-generated-id", "12345-0", "first-entry", "X");
        assertThat(RedisStreamsSourceEnumerator.extractLastGeneratedId(info)).isEqualTo("12345-0");

        assertThat(RedisStreamsSourceEnumerator.extractLastGeneratedId(List.of("length", 0L)))
                .isEqualTo("0-0");
        assertThat(RedisStreamsSourceEnumerator.extractLastGeneratedId(null)).isEqualTo("0-0");
    }
}
