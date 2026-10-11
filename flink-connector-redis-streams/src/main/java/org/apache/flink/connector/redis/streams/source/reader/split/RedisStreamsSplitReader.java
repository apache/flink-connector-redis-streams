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

package org.apache.flink.connector.redis.streams.source.reader.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.redis.streams.source.config.RedisStreamsSourceConfig;
import org.apache.flink.connector.redis.streams.source.config.StartupMode;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplit;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Split reader that fetches records from Redis Streams via consumer groups and defers XACK until
 * the next completed Flink checkpoint.
 */
@Internal
public class RedisStreamsSplitReader
        implements SplitReader<StreamMessage<String, String>, RedisStreamsSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisStreamsSplitReader.class);

    private static final long INITIAL_RECONNECT_DELAY_MS = 100L;
    private static final long MAX_RECONNECT_DELAY_MS = 30000L;
    private static final double BACKOFF_MULTIPLIER = 1.5;
    private static final int MAX_CONSUMER_GROUP_RETRIES = 3;
    private static final long CONSUMER_GROUP_RETRY_DELAY_MS = 100L;

    private final RedisStreamsSourceConfig config;
    private final int subtaskId;
    private final ReentrantLock stateLock = new ReentrantLock();

    // Owned by the fetch thread; commands is also read by the checkpoint thread under stateLock.
    private AbstractRedisClient redisClient;
    private StatefulConnection<String, String> connection;
    private RedisClusterCommands<String, String> commands;

    private final AtomicBoolean wokenUp = new AtomicBoolean(false);

    // Guarded by stateLock.

    private final Map<String, RedisStreamsSourceSplit> assignedSplits = new HashMap<>();
    private final Set<String> pausedSplits = new HashSet<>();
    private final Map<String, Deque<String>> deferredAcks = new HashMap<>();
    private final Map<String, CircuitBreaker> splitCircuitBreakers = new HashMap<>();
    // checkpointId -> splitId -> ordered list of IDs emitted before that barrier.
    // Populated via markCheckpoint() which is called from the task main thread; the IDs come
    // from the split states' deferredAckIds queue, which only contains EMITTED records.
    private final Map<Long, Map<String, List<String>>> checkpointAckSnapshots = new HashMap<>();
    private final Map<String, String> pendingRecoveryOffset = new HashMap<>();
    private final Set<String> pendingRecoveryComplete = new HashSet<>();
    private final Set<String> needsGroupInit = new HashSet<>();
    // Drops late/out-of-order notifyCheckpointComplete calls.
    private long maxCommittedCheckpointId = -1L;

    // Fetch-thread-only.
    private long nextReconnectDelayMs = INITIAL_RECONNECT_DELAY_MS;
    private long lastConnectionFailureTime = 0L;
    private boolean connectionInitialized = false;

    public RedisStreamsSplitReader(RedisStreamsSourceConfig config, int subtaskId) {
        this.config = config;
        this.subtaskId = subtaskId;
    }

    private void initializeConnection() {
        try {
            RedisClusterCommands<String, String> sync;
            if (config.isClusterMode()) {
                List<RedisURI> seeds = buildClusterSeedUris(config);
                RedisClusterClient cluster = RedisClusterClient.create(seeds);
                // Auto-reconnect off (we own backoff). Topology refresh on so MOVED/ASK and
                // failovers are picked up.
                ClusterTopologyRefreshOptions topology =
                        ClusterTopologyRefreshOptions.builder()
                                .enablePeriodicRefresh(
                                        Duration.ofMillis(
                                                config.getClusterTopologyRefreshPeriodMs()))
                                .enableAllAdaptiveRefreshTriggers()
                                .build();
                cluster.setOptions(
                        ClusterClientOptions.builder()
                                .autoReconnect(false)
                                .topologyRefreshOptions(topology)
                                .build());
                io.lettuce.core.cluster.api.StatefulRedisClusterConnection<String, String> conn =
                        cluster.connect();
                this.redisClient = cluster;
                this.connection = conn;
                sync = conn.sync();
                LOG.info(
                        "Connected to Redis Cluster (seeds={}, topologyRefresh={}ms)",
                        config.getClusterNodes(),
                        config.getClusterTopologyRefreshPeriodMs());
            } else {
                RedisURI.Builder uriBuilder =
                        RedisURI.builder()
                                .withHost(config.getHost())
                                .withPort(config.getPort())
                                .withDatabase(config.getDatabase())
                                .withTimeout(Duration.ofSeconds(5));
                if (config.getPassword() != null && !config.getPassword().isEmpty()) {
                    uriBuilder.withPassword(config.getPassword().toCharArray());
                }
                RedisClient client = RedisClient.create(uriBuilder.build());
                // Auto-reconnect off — we own backoff.
                client.setOptions(ClientOptions.builder().autoReconnect(false).build());
                io.lettuce.core.api.StatefulRedisConnection<String, String> conn = client.connect();
                this.redisClient = client;
                this.connection = conn;
                sync = conn.sync();
                LOG.info("Connected to Redis at {}:{}", config.getHost(), config.getPort());
            }
            stateLock.lock();
            try {
                this.commands = sync;
            } finally {
                stateLock.unlock();
            }
            this.nextReconnectDelayMs = INITIAL_RECONNECT_DELAY_MS;
            this.lastConnectionFailureTime = 0L;
            this.connectionInitialized = true;
        } catch (Exception e) {
            LOG.error("Failed to initialize Redis connection", e);
            cleanupConnection();
            handleConnectionFailure();
        }
    }

    private static List<RedisURI> buildClusterSeedUris(RedisStreamsSourceConfig cfg) {
        List<RedisURI> uris = new ArrayList<>(cfg.getClusterNodes().size());
        for (String node : cfg.getClusterNodes()) {
            int colon = node.lastIndexOf(':');
            String host = node.substring(0, colon);
            int port = Integer.parseInt(node.substring(colon + 1));
            RedisURI.Builder b =
                    RedisURI.builder()
                            .withHost(host)
                            .withPort(port)
                            .withTimeout(Duration.ofSeconds(5));
            if (cfg.getPassword() != null && !cfg.getPassword().isEmpty()) {
                b.withPassword(cfg.getPassword().toCharArray());
            }
            // Cluster does not honour SELECT, so database is always 0.
            uris.add(b.build());
        }
        return uris;
    }

    private void handleConnectionFailure() {
        lastConnectionFailureTime = System.currentTimeMillis();
        nextReconnectDelayMs =
                Math.min(
                        (long) (nextReconnectDelayMs * BACKOFF_MULTIPLIER), MAX_RECONNECT_DELAY_MS);
        LOG.warn("Connection failure detected. Will retry in {} ms", nextReconnectDelayMs);
    }

    private boolean ensureConnected() {
        try {
            if (!connectionInitialized || connection == null || !connection.isOpen()) {
                long timeSinceLastFailure = System.currentTimeMillis() - lastConnectionFailureTime;
                if (lastConnectionFailureTime > 0 && timeSinceLastFailure < nextReconnectDelayMs) {
                    return false;
                }
                LOG.info("Reconnecting to Redis (delay was {} ms)", nextReconnectDelayMs);
                cleanupConnection();
                initializeConnection();
                if (connection != null && connection.isOpen()) {
                    try {
                        commands.ping();
                        LOG.info("Redis connection re-established and verified");
                        return true;
                    } catch (Exception e) {
                        LOG.error("Redis health check failed after reconnect", e);
                        cleanupConnection();
                        handleConnectionFailure();
                        return false;
                    }
                }
                return false;
            }
            return true;
        } catch (Exception e) {
            LOG.error("Error checking connection status", e);
            cleanupConnection();
            handleConnectionFailure();
            return false;
        }
    }

    @Override
    public RecordsWithSplitIds<StreamMessage<String, String>> fetch() throws IOException {
        if (wokenUp.compareAndSet(true, false)) {
            return RedisStreamsRecords.empty();
        }

        retryPendingGroupInit();

        Map<String, RedisStreamsSourceSplit> splitsToFetch;
        stateLock.lock();
        try {
            if (assignedSplits.isEmpty()) {
                return RedisStreamsRecords.empty();
            }
            splitsToFetch = new HashMap<>();
            for (Map.Entry<String, RedisStreamsSourceSplit> entry : assignedSplits.entrySet()) {
                String splitId = entry.getKey();
                if (pausedSplits.contains(splitId)) {
                    continue;
                }
                if (needsGroupInit.contains(splitId)) {
                    continue;
                }
                CircuitBreaker breaker = splitCircuitBreakers.get(splitId);
                if (breaker != null && breaker.isOpen()) {
                    LOG.debug("Circuit breaker open for split {}, skipping fetch", splitId);
                    continue;
                }
                Deque<String> deferred = deferredAcks.get(splitId);
                if (deferred != null && deferred.size() >= config.getMaxDeferredAckQueueSize()) {
                    LOG.debug(
                            "Deferred ACK queue for split {} at capacity ({})",
                            splitId,
                            config.getMaxDeferredAckQueueSize());
                    continue;
                }
                splitsToFetch.put(splitId, entry.getValue());
            }
        } finally {
            stateLock.unlock();
        }

        if (splitsToFetch.isEmpty()) {
            return RedisStreamsRecords.empty();
        }

        if (!ensureConnected()) {
            LOG.warn(
                    "Redis connection unavailable; skipping fetch. Next retry in {} ms",
                    nextReconnectDelayMs);
            return RedisStreamsRecords.empty();
        }

        Map<String, Collection<StreamMessage<String, String>>> recordsBySplit = new HashMap<>();
        Set<String> finishedInThisFetch = new HashSet<>();

        for (Map.Entry<String, RedisStreamsSourceSplit> entry : splitsToFetch.entrySet()) {
            if (wokenUp.compareAndSet(true, false)) {
                break;
            }
            String splitId = entry.getKey();
            RedisStreamsSourceSplit split = entry.getValue();
            try {
                FetchResult result = fetchFromSplit(split);
                if (!result.messages.isEmpty()) {
                    recordsBySplit.put(splitId, result.messages);
                }
                if (result.finished) {
                    finishedInThisFetch.add(splitId);
                    LOG.info("Split {} reached its stopping bound", splitId);
                }
            } catch (RedisConnectionException e) {
                LOG.warn("Redis connection error for split {}: {}", splitId, e.getMessage());
                cleanupConnection();
                handleConnectionFailure();
                recordSplitFailure(splitId);
                break;
            } catch (Exception e) {
                if (e.getMessage() != null && e.getMessage().contains("NOGROUP")) {
                    LOG.warn("Consumer group missing for split {}; will retry init", splitId);
                    stateLock.lock();
                    try {
                        needsGroupInit.add(splitId);
                    } finally {
                        stateLock.unlock();
                    }
                } else {
                    LOG.error("Unexpected error fetching from split {}", splitId, e);
                    recordSplitFailure(splitId);
                }
            }
        }

        stateLock.lock();
        try {
            // Record circuit-breaker success for splits that returned data.
            for (String splitId : recordsBySplit.keySet()) {
                CircuitBreaker breaker = splitCircuitBreakers.get(splitId);
                if (breaker != null) {
                    breaker.recordSuccess();
                }
            }
            // Finished splits keep their deferred queue so the next checkpoint can XACK them.
            for (String splitId : finishedInThisFetch) {
                assignedSplits.remove(splitId);
                pendingRecoveryComplete.remove(splitId);
                pendingRecoveryOffset.remove(splitId);
                needsGroupInit.remove(splitId);
                pausedSplits.remove(splitId);
            }
        } finally {
            stateLock.unlock();
        }

        return new RedisStreamsRecords(recordsBySplit, finishedInThisFetch);
    }

    private void recordSplitFailure(String splitId) {
        stateLock.lock();
        try {
            splitCircuitBreakers
                    .computeIfAbsent(
                            splitId,
                            k ->
                                    new CircuitBreaker(
                                            config.getCircuitBreakerFailureThreshold(),
                                            config.getCircuitBreakerOpenDurationMs()))
                    .recordFailure();
        } finally {
            stateLock.unlock();
        }
    }

    private static final class FetchResult {
        static final FetchResult EMPTY_NOT_FINISHED =
                new FetchResult(Collections.emptyList(), false);

        final List<StreamMessage<String, String>> messages;
        final boolean finished;

        FetchResult(List<StreamMessage<String, String>> messages, boolean finished) {
            this.messages = messages;
            this.finished = finished;
        }
    }

    private FetchResult fetchFromSplit(RedisStreamsSourceSplit split) throws Exception {
        String streamKey = split.getStreamKey();
        String stoppingId = split.getStoppingEntryId();
        Consumer<String> consumer =
                Consumer.from(
                        config.getConsumerGroup(), config.getConsumerName() + "-" + subtaskId);

        boolean inRecovery;
        String pendingOffset;
        stateLock.lock();
        try {
            inRecovery = !pendingRecoveryComplete.contains(streamKey);
            pendingOffset = pendingRecoveryOffset.getOrDefault(streamKey, "0");
        } finally {
            stateLock.unlock();
        }

        if (inRecovery) {
            XReadArgs pendingReadArgs = XReadArgs.Builder.count(config.getBatchSize());
            List<StreamMessage<String, String>> pending =
                    commands.xreadgroup(
                            consumer,
                            pendingReadArgs,
                            XReadArgs.StreamOffset.from(streamKey, pendingOffset));

            if (pending != null && !pending.isEmpty()) {
                String lastPendingId = pending.get(pending.size() - 1).getId();
                // Defensive: bail out if the offset did not advance (would otherwise spin).
                if (lastPendingId.equals(pendingOffset)) {
                    LOG.warn(
                            "PEL recovery for split {} did not advance past offset {}",
                            streamKey,
                            pendingOffset);
                    completeRecovery(streamKey);
                    return new FetchResult(pending, isPastStop(stoppingId, lastPendingId));
                }
                // Re-acquire the lock to advance the recovery cursor atomically. Without the lock
                // a concurrent handleSplitsChanges() could reset pendingRecoveryOffset for this
                // split between the XREADGROUP call above and the cursor update here, causing
                // the recovery to restart from the wrong position.
                stateLock.lock();
                try {
                    pendingRecoveryOffset.put(streamKey, lastPendingId);
                } finally {
                    stateLock.unlock();
                }
                LOG.info(
                        "Recovered {} pending entries from PEL for split {} (up to {})",
                        pending.size(),
                        streamKey,
                        lastPendingId);
                if (pending.size() < config.getBatchSize()) {
                    completeRecovery(streamKey);
                    LOG.info("PEL recovery complete for split {}", streamKey);
                }
                return new FetchResult(pending, isPastStop(stoppingId, lastPendingId));
            }

            completeRecovery(streamKey);
            LOG.info("PEL recovery complete for split {} (no pending entries)", streamKey);
        }

        // Bounded mode blocks briefly so split completion is responsive.
        long blockMs =
                config.isBounded()
                        ? Math.min(config.getPollTimeout(), 200L)
                        : config.getPollTimeout();
        XReadArgs readArgs = XReadArgs.Builder.block(blockMs).count(config.getBatchSize());
        List<StreamMessage<String, String>> messages =
                commands.xreadgroup(
                        consumer, readArgs, XReadArgs.StreamOffset.lastConsumed(streamKey));

        if (messages == null || messages.isEmpty()) {
            return FetchResult.EMPTY_NOT_FINISHED;
        }

        // XREADGROUP moves every returned entry into the PEL even past stoppingId; XACK the tail
        // we discard to keep PEL aligned with what was actually consumed.
        if (stoppingId != null) {
            List<StreamMessage<String, String>> kept = new ArrayList<>(messages.size());
            List<String> discardIds = new ArrayList<>();
            boolean reached = false;
            for (StreamMessage<String, String> msg : messages) {
                int cmp = compareEntryIds(msg.getId(), stoppingId);
                if (cmp <= 0) {
                    kept.add(msg);
                    if (cmp == 0) {
                        reached = true;
                    }
                } else {
                    discardIds.add(msg.getId());
                    reached = true;
                }
            }
            if (!discardIds.isEmpty()) {
                try {
                    commands.xack(
                            streamKey,
                            config.getConsumerGroup(),
                            discardIds.toArray(new String[0]));
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to XACK {} out-of-bound entries for split {}; they remain in PEL",
                            discardIds.size(),
                            streamKey,
                            e);
                }
            }
            return new FetchResult(kept, reached);
        }
        return new FetchResult(messages, false);
    }

    private void completeRecovery(String streamKey) {
        stateLock.lock();
        try {
            pendingRecoveryComplete.add(streamKey);
            pendingRecoveryOffset.remove(streamKey);
        } finally {
            stateLock.unlock();
        }
    }

    private static boolean isPastStop(@Nullable String stoppingId, String entryId) {
        return stoppingId != null && compareEntryIds(entryId, stoppingId) >= 0;
    }

    /** Compare two {@code millis-seq} entry IDs. */
    @VisibleForTesting
    static int compareEntryIds(String a, String b) {
        long[] pa = parseEntryId(a);
        long[] pb = parseEntryId(b);
        int cmp = Long.compare(pa[0], pb[0]);
        return cmp != 0 ? cmp : Long.compare(pa[1], pb[1]);
    }

    private static long[] parseEntryId(String id) {
        int dash = id.indexOf('-');
        if (dash < 0) {
            return new long[] {Long.parseLong(id), 0L};
        }
        return new long[] {
            Long.parseLong(id.substring(0, dash)), Long.parseLong(id.substring(dash + 1))
        };
    }

    @Override
    public void handleSplitsChanges(SplitsChange<RedisStreamsSourceSplit> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    "Unsupported split change type: " + splitsChange.getClass());
        }

        List<RedisStreamsSourceSplit> additions = splitsChange.splits();
        stateLock.lock();
        try {
            LOG.info(
                    "Adding {} splits: {}",
                    additions.size(),
                    additions.stream().map(RedisStreamsSourceSplit::splitId).toArray());
            for (RedisStreamsSourceSplit split : additions) {
                String splitId = split.splitId();
                assignedSplits.put(splitId, split);
                deferredAcks.putIfAbsent(splitId, new ArrayDeque<>());
                splitCircuitBreakers.putIfAbsent(
                        splitId,
                        new CircuitBreaker(
                                config.getCircuitBreakerFailureThreshold(),
                                config.getCircuitBreakerOpenDurationMs()));
                // Re-enter PEL recovery on (re-)assignment in case the previous incarnation
                // left entries in the consumer's PEL.
                pendingRecoveryComplete.remove(splitId);
                pendingRecoveryOffset.remove(splitId);
                needsGroupInit.add(splitId);
            }
        } finally {
            stateLock.unlock();
        }

        // Eager group init; failures fall through to retryPendingGroupInit() in fetch().
        for (RedisStreamsSourceSplit split : additions) {
            if (tryInitConsumerGroup(split.getStreamKey())) {
                stateLock.lock();
                try {
                    needsGroupInit.remove(split.splitId());
                } finally {
                    stateLock.unlock();
                }
            }
        }
    }

    private void retryPendingGroupInit() {
        Set<String> toRetry;
        stateLock.lock();
        try {
            if (needsGroupInit.isEmpty()) {
                return;
            }
            toRetry = new HashSet<>(needsGroupInit);
        } finally {
            stateLock.unlock();
        }
        for (String splitId : toRetry) {
            if (tryInitConsumerGroup(splitId)) {
                stateLock.lock();
                try {
                    needsGroupInit.remove(splitId);
                } finally {
                    stateLock.unlock();
                }
            }
        }
    }

    private boolean tryInitConsumerGroup(String streamKey) {
        if (!ensureConnected()) {
            LOG.warn("Cannot init consumer group for {} — no connection", streamKey);
            return false;
        }
        String startOffset = config.getStartupMode() == StartupMode.EARLIEST ? "0-0" : "$";

        for (int attempt = 1; attempt <= MAX_CONSUMER_GROUP_RETRIES; attempt++) {
            try {
                commands.xgroupCreate(
                        XReadArgs.StreamOffset.from(streamKey, startOffset),
                        config.getConsumerGroup(),
                        XGroupCreateArgs.Builder.mkstream());
                LOG.info(
                        "Created consumer group {} for stream {} (startupMode={})",
                        config.getConsumerGroup(),
                        streamKey,
                        config.getStartupMode());
                return true;
            } catch (Exception e) {
                if (e.getMessage() != null && e.getMessage().contains("BUSYGROUP")) {
                    LOG.debug(
                            "Consumer group {} already exists for stream {}",
                            config.getConsumerGroup(),
                            streamKey);
                    return true;
                }
                if (attempt < MAX_CONSUMER_GROUP_RETRIES) {
                    LOG.debug(
                            "Group create attempt {}/{} failed for stream {}; retrying in {} ms",
                            attempt,
                            MAX_CONSUMER_GROUP_RETRIES,
                            streamKey,
                            CONSUMER_GROUP_RETRY_DELAY_MS);
                    try {
                        Thread.sleep(CONSUMER_GROUP_RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                } else {
                    LOG.error(
                            "Failed to create consumer group after {} attempts for stream {}",
                            MAX_CONSUMER_GROUP_RETRIES,
                            streamKey,
                            e);
                }
            }
        }
        return false;
    }

    @Override
    public void wakeUp() {
        wokenUp.set(true);
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<RedisStreamsSourceSplit> splitsToPause,
            Collection<RedisStreamsSourceSplit> splitsToResume) {
        stateLock.lock();
        try {
            for (RedisStreamsSourceSplit split : splitsToPause) {
                pausedSplits.add(split.splitId());
            }
            for (RedisStreamsSourceSplit split : splitsToResume) {
                pausedSplits.remove(split.splitId());
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Called by the task main thread at each checkpoint barrier. Receives the IDs of records that
     * were actually emitted to downstream (from split states' deferred-ACK queues) and stores them
     * for XACK after checkpoint completion.
     *
     * <p>The emitted IDs are also appended to {@link #deferredAcks} so that the back-pressure
     * check in {@link #fetch()} correctly reflects the number of pending ACKs.
     *
     * @param checkpointId Flink checkpoint ID
     * @param emittedIdsBySplit map of splitId → ordered list of entry IDs emitted before the
     *     barrier (drained from {@code RedisStreamsSourceSplitState.drainDeferredAckIds()})
     */
    public void markCheckpoint(long checkpointId, Map<String, List<String>> emittedIdsBySplit) {
        stateLock.lock();
        try {
            Map<String, List<String>> snapshot = new HashMap<>();
            for (Map.Entry<String, List<String>> entry : emittedIdsBySplit.entrySet()) {
                String splitId = entry.getKey();
                List<String> ids = entry.getValue();
                if (ids.isEmpty()) {
                    continue;
                }
                snapshot.put(splitId, new ArrayList<>(ids));
                // Append to deferredAcks for back-pressure accounting in fetch().
                Deque<String> deferred =
                        deferredAcks.computeIfAbsent(splitId, k -> new ArrayDeque<>());
                for (String id : ids) {
                    deferred.addLast(id);
                }
            }
            checkpointAckSnapshots.put(checkpointId, snapshot);
        } finally {
            stateLock.unlock();
        }
    }

    /** XACK messages that crossed the {@code checkpointId} barrier; late notifies are dropped. */
    public void acknowledgeAllPendingMessagesAtCheckpoint(long checkpointId) {
        Map<String, List<String>> snapshot;
        stateLock.lock();
        try {
            if (checkpointId <= maxCommittedCheckpointId) {
                LOG.debug(
                        "Dropping out-of-order notifyCheckpointComplete({}); already committed up to {}",
                        checkpointId,
                        maxCommittedCheckpointId);
                checkpointAckSnapshots.remove(checkpointId);
                return;
            }
            snapshot = checkpointAckSnapshots.remove(checkpointId);
            maxCommittedCheckpointId = checkpointId;

            // Evict orphaned snapshots for checkpoints < checkpointId whose discardCheckpoint()
            // was never called (e.g. JM crash mid-abort). Those IDs are still in deferredAcks
            // inflating back-pressure. Remove them to prevent a permanent memory leak; the PEL
            // will re-deliver the entries on the next recovery anyway.
            checkpointAckSnapshots.entrySet().removeIf(e -> {
                if (e.getKey() < checkpointId) {
                    Map<String, List<String>> orphaned = e.getValue();
                    if (orphaned != null) {
                        for (Map.Entry<String, List<String>> splitEntry : orphaned.entrySet()) {
                            Deque<String> deferred = deferredAcks.get(splitEntry.getKey());
                            if (deferred != null) {
                                for (String id : splitEntry.getValue()) {
                                    if (id.equals(deferred.peekFirst())) {
                                        deferred.removeFirst();
                                    } else {
                                        break;
                                    }
                                }
                            }
                        }
                        LOG.warn(
                                "Evicted orphaned checkpoint snapshot {} (discardCheckpoint was never called)",
                                e.getKey());
                    }
                    return true;
                }
                return false;
            });
        } finally {
            stateLock.unlock();
        }

        if (snapshot == null || snapshot.isEmpty()) {
            return;
        }

        for (Map.Entry<String, List<String>> entry : snapshot.entrySet()) {
            String splitId = entry.getKey();
            List<String> ids = entry.getValue();
            if (!ids.isEmpty()) {
                acknowledgeMessages(splitId, checkpointId, ids);
            }
        }

        cleanupAckedFinishedSplits();
    }

    /**
     * Discards the snapshot for an aborted checkpoint and removes the associated IDs from
     * {@link #deferredAcks} so they are re-captured at the next barrier.
     */
    public void discardCheckpoint(long checkpointId) {
        stateLock.lock();
        try {
            Map<String, List<String>> snapshot = checkpointAckSnapshots.remove(checkpointId);
            if (snapshot != null) {
                // Remove the aborted checkpoint's IDs from deferredAcks so that back-pressure
                // is not permanently inflated. They will be re-added when the next checkpoint
                // barrier drains the split states' deferredAckIds queues — but those IDs will
                // not be re-added from SplitState since drainDeferredAckIds() already cleared them
                // at the previous snapshotState(). The records themselves are safe: on recovery
                // Flink replays from the last successful checkpoint and PEL re-delivers them.
                for (Map.Entry<String, List<String>> entry : snapshot.entrySet()) {
                    Deque<String> deferred = deferredAcks.get(entry.getKey());
                    if (deferred != null) {
                        for (String id : entry.getValue()) {
                            String head = deferred.peekFirst();
                            if (id.equals(head)) {
                                deferred.removeFirst();
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    private boolean acknowledgeMessages(String splitId, long checkpointId, List<String> idsToAck) {
        if (idsToAck.isEmpty()) {
            return true;
        }
        String streamKey;
        RedisClusterCommands<String, String> cmds;

        stateLock.lock();
        try {
            cmds = commands;
            if (cmds == null) {
                LOG.warn(
                        "No connection for ACK of split {} at checkpoint {}; "
                                + "entries remain in PEL and will be re-delivered on recovery",
                        splitId,
                        checkpointId);
                return false;
            }
            RedisStreamsSourceSplit split = assignedSplits.get(splitId);
            // splitId == streamKey by design; fall back if split was already finished.
            streamKey = split != null ? split.getStreamKey() : splitId;
        } finally {
            stateLock.unlock();
        }

        try {
            cmds.xack(streamKey, config.getConsumerGroup(), idsToAck.toArray(new String[0]));
            LOG.debug("XACKed {} entries for split {} at checkpoint {}", idsToAck.size(), splitId, checkpointId);
        } catch (Exception e) {
            LOG.warn(
                    "XACK failed for split {} at checkpoint {} ({} ids); "
                                    + "entries remain in PEL and will be re-delivered on recovery",
                    splitId,
                    checkpointId,
                    idsToAck.size(),
                    e);
            return false;
        }

        stateLock.lock();
        try {
            Deque<String> deferred = deferredAcks.get(splitId);
            if (deferred == null) {
                return true;
            }
            // Remove front IDs by equality in strict FIFO order. A mismatch means the queue head
            // doesn't match the next expected ID to ACK — this should never happen in practice
            // because markCheckpoint() only adds IDs that RecordEmitter already emitted in order,
            // and acknowledgeMessages() is called with that same ordered list. A mismatch would
            // indicate a concurrent modification bug. We break rather than skip to avoid silently
            // leaving orphaned IDs in the back-pressure counter.
            for (String id : idsToAck) {
                String head = deferred.peekFirst();
                if (head == null) {
                    break;
                }
                if (head.equals(id)) {
                    deferred.removeFirst();
                } else {
                    LOG.warn(
                            "Unexpected head of deferred queue for split {}: expected {}, found {}. "
                                    + "This may indicate a concurrent modification bug.",
                            splitId,
                            id,
                            head);
                    break;
                }
            }
        } finally {
            stateLock.unlock();
        }
        return true;
    }

    private void cleanupAckedFinishedSplits() {
        stateLock.lock();
        try {
            Iterator<Map.Entry<String, Deque<String>>> it = deferredAcks.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Deque<String>> entry = it.next();
                String splitId = entry.getKey();
                if (!assignedSplits.containsKey(splitId) && entry.getValue().isEmpty()) {
                    it.remove();
                    splitCircuitBreakers.remove(splitId);
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing RedisStreamsSplitReader");
        cleanupConnection();
    }

    private void cleanupConnection() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.error("Error closing Redis connection", e);
        } finally {
            connection = null;
        }
        try {
            if (redisClient != null) {
                redisClient.shutdown();
            }
        } catch (Exception e) {
            LOG.error("Error shutting down Redis client", e);
        } finally {
            redisClient = null;
        }
        connectionInitialized = false;
        stateLock.lock();
        try {
            commands = null;
        } finally {
            stateLock.unlock();
        }
    }

    @VisibleForTesting
    Deque<String> getDeferredAcksForSplit(String splitId) {
        stateLock.lock();
        try {
            Deque<String> queue = deferredAcks.get(splitId);
            return queue == null ? null : new ArrayDeque<>(queue);
        } finally {
            stateLock.unlock();
        }
    }

    @VisibleForTesting
    long getMaxCommittedCheckpointId() {
        stateLock.lock();
        try {
            return maxCommittedCheckpointId;
        } finally {
            stateLock.unlock();
        }
    }

    /** Per-split circuit breaker; always accessed under {@link #stateLock}. */
    @VisibleForTesting
    static final class CircuitBreaker {
        private int consecutiveFailures = 0;
        private long circuitOpenTime = 0L;
        private final int failureThreshold;
        private final long openDurationMs;

        CircuitBreaker(int failureThreshold, long openDurationMs) {
            this.failureThreshold = failureThreshold;
            this.openDurationMs = openDurationMs;
        }

        void recordFailure() {
            consecutiveFailures++;
            if (consecutiveFailures >= failureThreshold) {
                circuitOpenTime = System.currentTimeMillis();
                LOG.warn(
                        "Circuit breaker opened after {} consecutive failures",
                        consecutiveFailures);
            }
        }

        void recordSuccess() {
            consecutiveFailures = 0;
            circuitOpenTime = 0L;
        }

        boolean isOpen() {
            if (circuitOpenTime == 0L) {
                return false;
            }
            if (System.currentTimeMillis() - circuitOpenTime > openDurationMs) {
                circuitOpenTime = 0L;
                consecutiveFailures = 0;
                return false;
            }
            return true;
        }
    }

    private static final class RedisStreamsRecords
            implements RecordsWithSplitIds<StreamMessage<String, String>> {

        private static final RedisStreamsRecords EMPTY =
                new RedisStreamsRecords(Collections.emptyMap(), Collections.emptySet());

        static RedisStreamsRecords empty() {
            return EMPTY;
        }

        private final Set<String> finishedSplits;
        private final Iterator<Map.Entry<String, Collection<StreamMessage<String, String>>>>
                splitIterator;

        @Nullable private Iterator<StreamMessage<String, String>> currentSplitIterator;

        RedisStreamsRecords(
                Map<String, Collection<StreamMessage<String, String>>> recordsBySplit,
                Set<String> finishedSplits) {
            this.finishedSplits = finishedSplits;
            this.splitIterator = recordsBySplit.entrySet().iterator();
        }

        @Nullable
        @Override
        public String nextSplit() {
            if (splitIterator.hasNext()) {
                Map.Entry<String, Collection<StreamMessage<String, String>>> entry =
                        splitIterator.next();
                currentSplitIterator = entry.getValue().iterator();
                return entry.getKey();
            }
            currentSplitIterator = null;
            return null;
        }

        @Nullable
        @Override
        public StreamMessage<String, String> nextRecordFromSplit() {
            if (currentSplitIterator != null && currentSplitIterator.hasNext()) {
                return currentSplitIterator.next();
            }
            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }
}
