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

import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.models.stream.PendingMessages;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link RedisStreamsSplitReader} against a Redis Cluster.
 *
 * <p>The local Docker daemon API is sometimes too old for the testcontainers version bundled
 * here, so the suite supports two backends:
 *
 * <ul>
 *   <li>{@code FLINK_REDIS_CLUSTER_NODES=host1:port1,host2:port2,...} env var pointing at an
 *       externally-managed cluster (e.g. {@code docker run -p 7000-7005:7000-7005
 *       grokzen/redis-cluster:7.0.10}).
 *   <li>Otherwise, {@code grokzen/redis-cluster:7.0.10} via testcontainers — skipped cleanly if
 *       Docker is unavailable or rejected by the docker-java client.
 * </ul>
 */
class RedisStreamsSplitReaderClusterTest {

    private static final String CLUSTER_IMAGE = "grokzen/redis-cluster:7.0.10";
    private static final String CONSUMER_GROUP = "cg";
    private static final String CONSUMER_NAME = "consumer";

    /** Stream key wrapped in a hash tag so all keys hash to the same slot for deterministic tests. */
    private static final String STREAM = "{tag}-stream-1";

    private static final String EXTERNAL_CLUSTER = System.getenv("FLINK_REDIS_CLUSTER_NODES");

    private static GenericContainer<?> CLUSTER;
    private static List<String> seedNodes;

    private RedisClusterClient client;
    private StatefulRedisClusterConnection<String, String> connection;
    private RedisAdvancedClusterCommands<String, String> commands;

    private RedisStreamsSplitReader currentReader;

    @BeforeAll
    static void startBackend() {
        if (EXTERNAL_CLUSTER != null && !EXTERNAL_CLUSTER.isEmpty()) {
            seedNodes = Arrays.asList(EXTERNAL_CLUSTER.split(","));
            return;
        }
        try {
            CLUSTER =
                    new GenericContainer<>(CLUSTER_IMAGE)
                            .withEnv("IP", "0.0.0.0")
                            .withEnv("INITIAL_PORT", "7000")
                            .withEnv("MASTERS", "3")
                            .withEnv("SLAVES_PER_MASTER", "0")
                            .withExposedPorts(7000, 7001, 7002, 7003, 7004, 7005);
            CLUSTER.start();
            seedNodes = new ArrayList<>();
            for (int port : new int[] {7000, 7001, 7002}) {
                seedNodes.add(CLUSTER.getHost() + ":" + CLUSTER.getMappedPort(port));
            }
        } catch (Throwable t) {
            CLUSTER = null;
            Assumptions.abort(
                    "No Redis Cluster backend available — set FLINK_REDIS_CLUSTER_NODES or run a "
                            + "modern Docker daemon. Cause: "
                            + t.getMessage());
        }
    }

    @AfterAll
    static void stopBackend() {
        if (CLUSTER != null) {
            CLUSTER.stop();
        }
    }

    @BeforeEach
    void openConnection() {
        List<RedisURI> uris =
                seedNodes.stream()
                        .map(
                                hp -> {
                                    int colon = hp.lastIndexOf(':');
                                    return RedisURI.builder()
                                            .withHost(hp.substring(0, colon))
                                            .withPort(Integer.parseInt(hp.substring(colon + 1)))
                                            .withTimeout(Duration.ofSeconds(5))
                                            .build();
                                })
                        .collect(Collectors.toList());
        client = RedisClusterClient.create(uris);
        connection = client.connect();
        commands = connection.sync();
        // Best-effort cleanup: delete the test stream and its consumer group on each test start.
        try {
            commands.del(STREAM);
        } catch (Exception ignored) {
            // ok if the key does not exist
        }
    }

    @AfterEach
    void cleanupReader() throws Exception {
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (Exception ignored) {
                // best-effort
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
                .setClusterNodes(seedNodes)
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

    private static void markCheckpoint(
            RedisStreamsSplitReader reader, long checkpointId, List<String> emittedIds) {
        Map<String, List<String>> perSplit =
                emittedIds.isEmpty() ? Map.of() : Map.of(STREAM, emittedIds);
        reader.markCheckpoint(checkpointId, perSplit);
    }

    @Test
    void clusterEndToEndFetchAckCycle() throws Exception {
        RedisStreamsSplitReader reader = newReader(cfg().setMaxDeferredAckQueueSize(100).build());
        reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(new RedisStreamsSourceSplit(STREAM))));

        // Add records to the cluster — Lettuce hashes "{tag}-stream-1" by the {tag} hash-tag, so
        // the entries land on a single shard. Cluster XADD is automatically routed to the slot
        // owner.
        List<String> producedIds = new ArrayList<>();
        producedIds.add(commands.xadd(STREAM, Map.of("k", "v1")));
        producedIds.add(commands.xadd(STREAM, Map.of("k", "v2")));
        producedIds.add(commands.xadd(STREAM, Map.of("k", "v3")));

        List<String> fetched = idsOf(reader.fetch());
        assertThat(fetched).containsExactlyElementsOf(producedIds);
        assertThat(pelCount()).isEqualTo(3);

        // Drive a checkpoint cycle and verify XACK lands.
        markCheckpoint(reader, 1L, fetched);
        reader.acknowledgeAllPendingMessagesAtCheckpoint(1L);

        assertThat(reader.getDeferredAcksForSplit(STREAM)).isEmpty();
        assertThat(pelCount()).as("XACK against cluster drains the PEL").isZero();
    }

    @Test
    void clusterMultipleCheckpointCycles() throws Exception {
        RedisStreamsSplitReader reader = newReader(cfg().setMaxDeferredAckQueueSize(100).build());
        reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(new RedisStreamsSourceSplit(STREAM))));

        // Three independent burst+checkpoint cycles to confirm the snapshot-as-limit ACK
        // semantics also hold against cluster mode (the original bug would surface here too).
        for (int cycle = 1; cycle <= 3; cycle++) {
            for (int i = 0; i < cycle + 1; i++) {
                commands.xadd(STREAM, Map.of("c", String.valueOf(cycle), "i", String.valueOf(i)));
            }
            List<String> batchIds = idsOf(reader.fetch());
            markCheckpoint(reader, cycle, batchIds);
            reader.acknowledgeAllPendingMessagesAtCheckpoint(cycle);

            assertThat(reader.getDeferredAcksForSplit(STREAM))
                    .as("cycle %d: deferred queue drained", cycle)
                    .isEmpty();
            assertThat(pelCount()).as("cycle %d: PEL drained", cycle).isZero();
        }
    }

    @Test
    void clusterPelRecoveryAcrossRestart() throws Exception {
        // Reader A: fetches but doesn't ACK before close → entries left in PEL on the shard.
        RedisStreamsSplitReader readerA = newReader(cfg().build());
        for (int i = 0; i < 5; i++) {
            commands.xadd(STREAM, Map.of("k", "pre-" + i));
        }
        readerA.handleSplitsChanges(
                new SplitsAddition<>(List.of(new RedisStreamsSourceSplit(STREAM))));
        List<String> firstFetch = idsOf(readerA.fetch());
        assertThat(firstFetch).hasSize(5);
        assertThat(pelCount()).isEqualTo(5);
        readerA.close();
        currentReader = null;

        // Reader B: connects to the cluster fresh, must drain the PEL via consumer-group recovery
        // before it sees new entries.
        RedisStreamsSplitReader readerB = newReader(cfg().build());
        readerB.handleSplitsChanges(
                new SplitsAddition<>(List.of(new RedisStreamsSourceSplit(STREAM))));
        List<String> recovered = idsOf(readerB.fetch());
        assertThat(recovered).containsExactlyElementsOf(firstFetch);

        commands.xadd(STREAM, Map.of("k", "post"));
        List<String> steady = idsOf(readerB.fetch());
        assertThat(steady).hasSize(1).doesNotContainAnyElementsOf(firstFetch);

        List<String> allRecoveredAndSteady = new ArrayList<>(recovered);
        allRecoveredAndSteady.addAll(steady);
        markCheckpoint(readerB, 1L, allRecoveredAndSteady);
        readerB.acknowledgeAllPendingMessagesAtCheckpoint(1L);
        assertThat(pelCount()).isZero();
    }
}
