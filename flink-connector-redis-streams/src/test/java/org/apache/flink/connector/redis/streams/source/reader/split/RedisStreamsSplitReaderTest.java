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

package org.apache.flink.connector.redis.streams.source.reader.split;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.redis.streams.source.config.RedisStreamsSourceConfig;
import org.apache.flink.connector.redis.streams.source.config.StartupMode;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplit;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.models.stream.PendingMessages;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link RedisStreamsSplitReader} against a real Redis container. */
class RedisStreamsSplitReaderTest {

    private static final String REDIS_IMAGE = "redis:7-alpine";
    private static final int REDIS_PORT = 6379;
    private static final String STREAM = "stream-1";
    private static final String CONSUMER_GROUP = "cg";
    private static final String CONSUMER_NAME = "consumer";

    /**
     * Override to point at an externally-managed Redis (host:port) when testcontainers cannot reach
     * the local Docker daemon. Useful for local development on machines whose docker client/daemon
     * is older than the docker-java client baked into testcontainers.
     */
    private static final String EXTERNAL_REDIS = System.getenv("FLINK_REDIS_TEST_HOSTPORT");

    private static GenericContainer<?> REDIS;
    private static String redisHost;
    private static int redisPort;

    private RedisClient client;
    private StatefulRedisConnection<String, String> connection;
    private RedisCommands<String, String> commands;

    private RedisStreamsSplitReader currentReader;

    @BeforeAll
    static void startBackend() {
        if (EXTERNAL_REDIS != null && !EXTERNAL_REDIS.isEmpty()) {
            String[] hp = EXTERNAL_REDIS.split(":", 2);
            redisHost = hp[0];
            redisPort = Integer.parseInt(hp[1]);
            return;
        }
        try {
            REDIS = new GenericContainer<>(REDIS_IMAGE).withExposedPorts(REDIS_PORT);
            REDIS.start();
            redisHost = REDIS.getHost();
            redisPort = REDIS.getMappedPort(REDIS_PORT);
        } catch (Throwable t) {
            REDIS = null;
            Assumptions.abort(
                    "No Redis backend available — set FLINK_REDIS_TEST_HOSTPORT or run a "
                            + "modern Docker daemon. Cause: "
                            + t.getMessage());
        }
    }

    @AfterAll
    static void stopBackend() {
        if (REDIS != null) {
            REDIS.stop();
        }
    }

    @BeforeEach
    void openConnection() {
        client =
                RedisClient.create(
                        RedisURI.builder().withHost(redisHost).withPort(redisPort).build());
        connection = client.connect();
        commands = connection.sync();
        commands.flushdb();
    }

    @AfterEach
    void cleanupReader() throws Exception {
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (Exception ignored) {
            }
            currentReader = null;
        }
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }

    private RedisStreamsSourceConfig.Builder cfg() {
        return RedisStreamsSourceConfig.builder()
                .setHost(redisHost)
                .setPort(redisPort)
                .setConsumerGroup(CONSUMER_GROUP)
                .setConsumerName(CONSUMER_NAME)
                .setStreamKeys(List.of(STREAM))
                .setStartupMode(StartupMode.EARLIEST)
                .setPollTimeout(200);
    }

    private RedisStreamsSplitReader newReader(RedisStreamsSourceConfig config) {
        currentReader = new RedisStreamsSplitReader(config, 0);
        return currentReader;
    }

    private static RedisStreamsSourceSplit unboundedSplit() {
        return new RedisStreamsSourceSplit(STREAM);
    }

    private static List<String> idsOf(RecordsWithSplitIds<StreamMessage<String, String>> records) {
        List<String> ids = new ArrayList<>();
        String splitId = records.nextSplit();
        while (splitId != null) {
            StreamMessage<String, String> msg = records.nextRecordFromSplit();
            while (msg != null) {
                ids.add(msg.getId());
                msg = records.nextRecordFromSplit();
            }
            splitId = records.nextSplit();
        }
        return ids;
    }

    private long pelCount() {
        PendingMessages pending = commands.xpending(STREAM, CONSUMER_GROUP);
        return pending == null ? 0 : pending.getCount();
    }

    /**
     * Simulates what {@code RedisStreamsSourceReader.snapshotState()} does in the full pipeline:
     * drains emitted IDs from split states and hands them to the split reader. In unit tests that
     * drive the SplitReader directly (without the SourceReader/RecordEmitter), we pass the IDs
     * returned by {@link #idsOf} as if they had already been emitted.
     */
    private static void markCheckpoint(
            RedisStreamsSplitReader reader, long checkpointId, List<String> emittedIds) {
        Map<String, List<String>> perSplit =
                emittedIds.isEmpty() ? Map.of() : Map.of(STREAM, emittedIds);
        reader.markCheckpoint(checkpointId, perSplit);
    }

    @Test
    void backpressureSkipsFetchWhenQueueAtCapacity() throws Exception {
        RedisStreamsSplitReader reader =
                newReader(cfg().setBatchSize(2).setMaxDeferredAckQueueSize(2).build());

        commands.xadd(STREAM, Map.of("f", "1"));
        commands.xadd(STREAM, Map.of("f", "2"));

        reader.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));
        List<String> firstBatch = idsOf(reader.fetch());

        // Simulate checkpoint: transfers fetched IDs into SplitReader's deferredAcks so that
        // the back-pressure check (deferredAcks.size() >= maxDeferredAckQueueSize) kicks in.
        markCheckpoint(reader, 1L, firstBatch);

        Deque<String> deferredAfterFirst = reader.getDeferredAcksForSplit(STREAM);
        assertThat(deferredAfterFirst).hasSize(2);

        commands.xadd(STREAM, Map.of("f", "3"));
        commands.xadd(STREAM, Map.of("f", "4"));
        idsOf(reader.fetch()); // should be skipped — queue at capacity

        assertThat(reader.getDeferredAcksForSplit(STREAM))
                .containsExactlyElementsOf(deferredAfterFirst);
    }

    @Test
    void multipleCheckpointCyclesAckProgressively() throws Exception {
        RedisStreamsSplitReader reader = newReader(cfg().setMaxDeferredAckQueueSize(100).build());
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));

        commands.xadd(STREAM, Map.of("k", "a1"));
        commands.xadd(STREAM, Map.of("k", "a2"));
        List<String> batch1 = idsOf(reader.fetch());
        markCheckpoint(reader, 1L, batch1);
        reader.acknowledgeAllPendingMessagesAtCheckpoint(1L);

        assertThat(reader.getDeferredAcksForSplit(STREAM)).isEmpty();
        assertThat(pelCount()).isZero();

        commands.xadd(STREAM, Map.of("k", "b1"));
        commands.xadd(STREAM, Map.of("k", "b2"));
        commands.xadd(STREAM, Map.of("k", "b3"));
        List<String> batch2 = idsOf(reader.fetch());
        markCheckpoint(reader, 2L, batch2);
        reader.acknowledgeAllPendingMessagesAtCheckpoint(2L);

        assertThat(reader.getDeferredAcksForSplit(STREAM)).isEmpty();
        assertThat(pelCount()).isZero();

        for (int i = 0; i < 5; i++) {
            commands.xadd(STREAM, Map.of("k", "c" + i));
        }
        List<String> batch3 = idsOf(reader.fetch());
        markCheckpoint(reader, 3L, batch3);
        reader.acknowledgeAllPendingMessagesAtCheckpoint(3L);

        assertThat(reader.getDeferredAcksForSplit(STREAM)).isEmpty();
        assertThat(pelCount()).isZero();
    }

    @Test
    void messagesArrivingAfterBarrierAreNotAckedUntilNextCheckpoint() throws Exception {
        RedisStreamsSplitReader reader = newReader(cfg().build());
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));

        commands.xadd(STREAM, Map.of("k", "barrier-1"));
        commands.xadd(STREAM, Map.of("k", "barrier-2"));
        List<String> preBarrier = idsOf(reader.fetch());
        // Checkpoint 1 snapshot: only the 2 pre-barrier IDs.
        markCheckpoint(reader, 1L, preBarrier);

        // Post-barrier records arrive and are fetched — but NOT included in checkpoint 1.
        commands.xadd(STREAM, Map.of("k", "post-barrier"));
        idsOf(reader.fetch());
        // Do NOT call markCheckpoint for post-barrier records — they belong to checkpoint 2.

        reader.acknowledgeAllPendingMessagesAtCheckpoint(1L);

        // Pre-barrier IDs are XACKed; post-barrier ID is still in PEL.
        assertThat(pelCount()).isEqualTo(1);
    }

    @Test
    void abortedCheckpointDoesNotAck() throws Exception {
        RedisStreamsSplitReader reader = newReader(cfg().build());
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));

        commands.xadd(STREAM, Map.of("k", "x"));
        commands.xadd(STREAM, Map.of("k", "y"));
        List<String> batch = idsOf(reader.fetch());

        markCheckpoint(reader, 10L, batch);
        reader.discardCheckpoint(10L);
        reader.acknowledgeAllPendingMessagesAtCheckpoint(10L); // snapshot gone → no-op

        // Both IDs must still be in the PEL (not XACKed).
        assertThat(pelCount()).isEqualTo(2);
    }

    @Test
    void outOfOrderNotifyCheckpointCompleteIsDropped() throws Exception {
        RedisStreamsSplitReader reader = newReader(cfg().build());
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));

        commands.xadd(STREAM, Map.of("k", "1"));
        commands.xadd(STREAM, Map.of("k", "2"));
        List<String> batch = idsOf(reader.fetch());

        markCheckpoint(reader, 5L, batch);
        reader.acknowledgeAllPendingMessagesAtCheckpoint(5L);
        assertThat(reader.getMaxCommittedCheckpointId()).isEqualTo(5L);

        // Checkpoint 3 arrives late — must be dropped (no double XACK, no state corruption).
        markCheckpoint(reader, 3L, List.of());
        reader.acknowledgeAllPendingMessagesAtCheckpoint(3L);
        assertThat(reader.getMaxCommittedCheckpointId()).isEqualTo(5L);
    }

    @Test
    void pelRecoverySmallBatch() throws Exception {
        RedisStreamsSplitReader readerA = newReader(cfg().build());
        commands.xadd(STREAM, Map.of("k", "v1"));
        commands.xadd(STREAM, Map.of("k", "v2"));
        readerA.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));
        List<String> firstFetch = idsOf(readerA.fetch());
        assertThat(firstFetch).hasSize(2);
        assertThat(pelCount()).isEqualTo(2);
        readerA.close();
        currentReader = null;

        RedisStreamsSplitReader readerB = newReader(cfg().build());
        readerB.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));
        List<String> recovered = idsOf(readerB.fetch());
        assertThat(recovered).containsExactlyElementsOf(firstFetch);

        commands.xadd(STREAM, Map.of("k", "v3"));
        List<String> steady = idsOf(readerB.fetch());
        assertThat(steady).hasSize(1).doesNotContainAnyElementsOf(firstFetch);

        List<String> allRecoveredAndSteady = new ArrayList<>(recovered);
        allRecoveredAndSteady.addAll(steady);
        markCheckpoint(readerB, 1L, allRecoveredAndSteady);
        readerB.acknowledgeAllPendingMessagesAtCheckpoint(1L);
        assertThat(pelCount()).isZero();
    }

    @Test
    void pelRecoveryLargerThanBatch() throws Exception {
        int total = 250;
        int batch = 100;

        RedisStreamsSplitReader readerA = newReader(cfg().setBatchSize(batch).build());
        for (int i = 0; i < total; i++) {
            commands.xadd(STREAM, Map.of("k", String.valueOf(i)));
        }
        readerA.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));
        List<String> all = new ArrayList<>();
        for (int i = 0; i < 5 && all.size() < total; i++) {
            all.addAll(idsOf(readerA.fetch()));
        }
        assertThat(all).hasSize(total);
        assertThat(pelCount()).isEqualTo(total);
        readerA.close();
        currentReader = null;

        RedisStreamsSplitReader readerB = newReader(cfg().setBatchSize(batch).build());
        readerB.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));
        List<String> recovered = new ArrayList<>();
        for (int i = 0; i < 10 && recovered.size() < total; i++) {
            recovered.addAll(idsOf(readerB.fetch()));
        }
        assertThat(recovered).containsExactlyElementsOf(all);

        markCheckpoint(readerB, 1L, recovered);
        readerB.acknowledgeAllPendingMessagesAtCheckpoint(1L);
        assertThat(pelCount()).isZero();
    }

    @Test
    void boundedSplitFinishesAtStoppingId() throws Exception {
        commands.xadd(STREAM, Map.of("k", "a"));
        String stoppingId = commands.xadd(STREAM, Map.of("k", "b"));
        commands.xadd(STREAM, Map.of("k", "c"));

        RedisStreamsSplitReader reader = newReader(cfg().setBounded(true).build());
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        List.of(new RedisStreamsSourceSplit(STREAM, null, stoppingId))));

        List<String> consumed = new ArrayList<>();
        boolean finished = false;
        for (int i = 0; i < 5 && !finished; i++) {
            RecordsWithSplitIds<StreamMessage<String, String>> records = reader.fetch();
            consumed.addAll(idsOf(records));
            finished = records.finishedSplits().contains(STREAM);
        }
        assertThat(finished).isTrue();
        assertThat(consumed).hasSize(2).last().isEqualTo(stoppingId);
    }

    @Test
    void deferredAcksForFinishedSplitDrainAtNextCheckpoint() throws Exception {
        String stoppingId = commands.xadd(STREAM, Map.of("k", "a"));
        commands.xadd(STREAM, Map.of("k", "b"));

        RedisStreamsSplitReader reader = newReader(cfg().setBounded(true).build());
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        List.of(new RedisStreamsSourceSplit(STREAM, null, stoppingId))));

        boolean finished = false;
        List<String> allIds = new ArrayList<>();
        for (int i = 0; i < 5 && !finished; i++) {
            RecordsWithSplitIds<StreamMessage<String, String>> r = reader.fetch();
            allIds.addAll(idsOf(r));
            finished = r.finishedSplits().contains(STREAM);
        }
        assertThat(finished).isTrue();
        assertThat(pelCount()).isEqualTo(1);

        markCheckpoint(reader, 1L, allIds);
        reader.acknowledgeAllPendingMessagesAtCheckpoint(1L);

        assertThat(pelCount()).isZero();
    }

    @Test
    void addSplitsBackReentersPelRecovery() throws Exception {
        RedisStreamsSplitReader readerA = newReader(cfg().build());
        commands.xadd(STREAM, Map.of("k", "stale"));
        readerA.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));
        idsOf(readerA.fetch());
        assertThat(pelCount()).isEqualTo(1);
        readerA.close();
        currentReader = null;

        RedisStreamsSplitReader readerB = newReader(cfg().build());
        readerB.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));
        readerB.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));

        List<String> recovered = idsOf(readerB.fetch());
        assertThat(recovered).hasSize(1);
    }

    @Test
    void startupModeLatestSkipsPreExistingEntries() throws Exception {
        // Add messages BEFORE the consumer group exists
        commands.xadd(STREAM, Map.of("k", "old-1"));
        commands.xadd(STREAM, Map.of("k", "old-2"));

        // LATEST startup mode creates the consumer group at $ (current stream tip),
        // so pre-existing entries are invisible to the group.
        RedisStreamsSplitReader reader =
                newReader(cfg().setStartupMode(StartupMode.LATEST).build());
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(unboundedSplit())));

        // First fetch must be empty — old messages are before the group's start position.
        assertThat(idsOf(reader.fetch())).isEmpty();

        // A new message published after the group is created must be delivered.
        String newId = commands.xadd(STREAM, Map.of("k", "new-1"));
        assertThat(idsOf(reader.fetch())).containsExactly(newId);
    }

    @Test
    void compareEntryIdsOrdersByMillisThenSequence() {
        assertThat(RedisStreamsSplitReader.compareEntryIds("100-0", "100-0")).isZero();
        assertThat(RedisStreamsSplitReader.compareEntryIds("100-0", "100-1")).isNegative();
        assertThat(RedisStreamsSplitReader.compareEntryIds("100-2", "100-1")).isPositive();
        assertThat(RedisStreamsSplitReader.compareEntryIds("99-9", "100-0")).isNegative();
        assertThat(RedisStreamsSplitReader.compareEntryIds("100-0", "99-9999")).isPositive();
        assertThat(RedisStreamsSplitReader.compareEntryIds("100", "100-0")).isZero();
    }
}
