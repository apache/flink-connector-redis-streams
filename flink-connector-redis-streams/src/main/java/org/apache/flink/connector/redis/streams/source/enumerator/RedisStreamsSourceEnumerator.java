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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.redis.streams.source.config.RedisStreamsSourceConfig;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplit;
import org.apache.flink.util.FlinkRuntimeException;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Enumerator for the Redis Streams Source. One split per stream key, round-robin assignment.
 * Bounded mode freezes a stopping entry ID per key via {@code XINFO STREAM} at startup.
 */
@Internal
public class RedisStreamsSourceEnumerator
        implements SplitEnumerator<RedisStreamsSourceSplit, RedisStreamsSourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisStreamsSourceEnumerator.class);

    private final RedisStreamsSourceConfig sourceConfig;
    private final SplitEnumeratorContext<RedisStreamsSourceSplit> context;

    // SplitEnumerator callbacks are serialized by the SourceCoordinator event loop, so no
    // synchronization on this state is required.
    private final Set<String> pendingSplitKeys;
    private final Map<Integer, Set<String>> readerAssignments = new HashMap<>();
    private final Set<Integer> readersSignaledNoMoreSplits = new HashSet<>();
    private final Map<String, String> stoppingEntryIds = new HashMap<>();
    private final Function<String, String> lastGeneratedIdLookup;

    public RedisStreamsSourceEnumerator(
            RedisStreamsSourceConfig sourceConfig,
            SplitEnumeratorContext<RedisStreamsSourceSplit> context,
            @Nullable RedisStreamsSourceEnumeratorState restoredState) {
        this(sourceConfig, context, restoredState, defaultLookup(sourceConfig));
    }

    @VisibleForTesting
    RedisStreamsSourceEnumerator(
            RedisStreamsSourceConfig sourceConfig,
            SplitEnumeratorContext<RedisStreamsSourceSplit> context,
            @Nullable RedisStreamsSourceEnumeratorState restoredState,
            Function<String, String> lastGeneratedIdLookup) {
        this.sourceConfig = sourceConfig;
        this.context = context;
        this.lastGeneratedIdLookup = lastGeneratedIdLookup;
        this.pendingSplitKeys = new HashSet<>();
        if (restoredState != null) {
            this.pendingSplitKeys.addAll(restoredState.getPendingSplits());
            this.stoppingEntryIds.putAll(restoredState.getStoppingEntryIds());
        } else {
            this.pendingSplitKeys.addAll(sourceConfig.getStreamKeys());
        }
    }

    @Override
    public void start() {
        LOG.info("Starting Redis Streams Source Enumerator (bounded={})", sourceConfig.isBounded());
        if (sourceConfig.isBounded()) {
            for (String streamKey : sourceConfig.getStreamKeys()) {
                if (stoppingEntryIds.containsKey(streamKey)) {
                    continue; // restored from checkpoint; do not re-query XINFO.
                }
                String stoppingId = lastGeneratedIdLookup.apply(streamKey);
                stoppingEntryIds.put(streamKey, stoppingId);
                LOG.info("Bounded mode: stream {} stopping entry id = {}", streamKey, stoppingId);
            }
        }
        dispatchAssignments(buildAssignments());
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.debug("Received split request from reader {}", subtaskId);
        dispatchAssignments(buildAssignments());
    }

    @Override
    public void addSplitsBack(List<RedisStreamsSourceSplit> splits, int subtaskId) {
        LOG.info("Adding {} splits back from reader {}", splits.size(), subtaskId);
        Set<String> assigned = readerAssignments.get(subtaskId);
        if (assigned != null) {
            for (RedisStreamsSourceSplit split : splits) {
                assigned.remove(split.splitId());
            }
            if (assigned.isEmpty()) {
                readerAssignments.remove(subtaskId);
            }
        }
        readersSignaledNoMoreSplits.remove(subtaskId);
        for (RedisStreamsSourceSplit split : splits) {
            pendingSplitKeys.add(split.getStreamKey());
        }
        dispatchAssignments(buildAssignments());
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Adding reader {}", subtaskId);
        dispatchAssignments(buildAssignments());
        if (sourceConfig.isBounded()
                && pendingSplitKeys.isEmpty()
                && !readersSignaledNoMoreSplits.contains(subtaskId)) {
            readersSignaledNoMoreSplits.add(subtaskId);
            context.signalNoMoreSplits(subtaskId);
            LOG.info("Signaled no more splits for reader {}", subtaskId);
        }
    }

    @Override
    public RedisStreamsSourceEnumeratorState snapshotState(long checkpointId) {
        LOG.debug("Snapshotting state for checkpoint {}", checkpointId);
        return new RedisStreamsSourceEnumeratorState(
                new HashSet<>(pendingSplitKeys), new HashMap<>(stoppingEntryIds));
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing Redis Streams Source Enumerator");
    }

    private Map<Integer, List<RedisStreamsSourceSplit>> buildAssignments() {
        if (pendingSplitKeys.isEmpty()) {
            return Collections.emptyMap();
        }
        List<Integer> readers = new ArrayList<>(context.registeredReaders().keySet());
        if (readers.isEmpty()) {
            LOG.debug("No readers available for split assignment");
            return Collections.emptyMap();
        }
        Map<Integer, List<RedisStreamsSourceSplit>> assignments = new HashMap<>();
        int idx = 0;
        for (String streamKey : new ArrayList<>(pendingSplitKeys)) {
            int readerId = readers.get(idx % readers.size());
            String stoppingId =
                    sourceConfig.isBounded() ? stoppingEntryIds.get(streamKey) : null;
            RedisStreamsSourceSplit split =
                    new RedisStreamsSourceSplit(streamKey, null, stoppingId);
            assignments.computeIfAbsent(readerId, k -> new ArrayList<>()).add(split);
            readerAssignments.computeIfAbsent(readerId, k -> new HashSet<>()).add(streamKey);
            pendingSplitKeys.remove(streamKey);
            idx++;
        }
        return assignments;
    }

    private void dispatchAssignments(Map<Integer, List<RedisStreamsSourceSplit>> assignments) {
        for (Map.Entry<Integer, List<RedisStreamsSourceSplit>> entry : assignments.entrySet()) {
            int readerId = entry.getKey();
            List<RedisStreamsSourceSplit> splits = entry.getValue();
            LOG.info("Assigning {} splits to reader {}: {}", splits.size(), readerId, splits);
            context.assignSplits(new SplitsAssignment<>(Map.of(readerId, splits)));

            if (sourceConfig.isBounded()
                    && pendingSplitKeys.isEmpty()
                    && !readersSignaledNoMoreSplits.contains(readerId)) {
                readersSignaledNoMoreSplits.add(readerId);
                context.signalNoMoreSplits(readerId);
                LOG.info("Signaled no more splits for reader {}", readerId);
            }
        }
    }

    /** Resolves {@code XINFO STREAM <key>}'s {@code last-generated-id}; "0-0" if the stream is missing. */
    private static Function<String, String> defaultLookup(RedisStreamsSourceConfig cfg) {
        return streamKey -> {
            try {
                if (cfg.isClusterMode()) {
                    return lookupViaCluster(cfg, streamKey);
                }
                return lookupViaStandalone(cfg, streamKey);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to query XINFO STREAM for bounded mode setup of " + streamKey, e);
            }
        };
    }

    private static String lookupViaStandalone(RedisStreamsSourceConfig cfg, String streamKey) {
        RedisURI.Builder uriBuilder =
                RedisURI.builder()
                        .withHost(cfg.getHost())
                        .withPort(cfg.getPort())
                        .withDatabase(cfg.getDatabase())
                        .withTimeout(Duration.ofSeconds(5));
        if (cfg.getPassword() != null && !cfg.getPassword().isEmpty()) {
            uriBuilder.withPassword(cfg.getPassword().toCharArray());
        }
        try (RedisClient client = RedisClient.create(uriBuilder.build())) {
            client.setOptions(ClientOptions.builder().autoReconnect(false).build());
            try (StatefulRedisConnection<String, String> conn = client.connect()) {
                return runXinfo(conn.sync(), streamKey);
            }
        }
    }

    private static String lookupViaCluster(RedisStreamsSourceConfig cfg, String streamKey) {
        List<RedisURI> seeds = new ArrayList<>(cfg.getClusterNodes().size());
        for (String node : cfg.getClusterNodes()) {
            int colon = node.lastIndexOf(':');
            RedisURI.Builder b =
                    RedisURI.builder()
                            .withHost(node.substring(0, colon))
                            .withPort(Integer.parseInt(node.substring(colon + 1)))
                            .withTimeout(Duration.ofSeconds(5));
            if (cfg.getPassword() != null && !cfg.getPassword().isEmpty()) {
                b.withPassword(cfg.getPassword().toCharArray());
            }
            seeds.add(b.build());
        }
        try (RedisClusterClient client = RedisClusterClient.create(seeds)) {
            client.setOptions(ClusterClientOptions.builder().autoReconnect(false).build());
            try (StatefulRedisClusterConnection<String, String> conn = client.connect()) {
                return runXinfo(conn.sync(), streamKey);
            }
        }
    }

    private static String runXinfo(RedisClusterCommands<String, String> cmds, String streamKey) {
        try {
            return extractLastGeneratedId(cmds.xinfoStream(streamKey));
        } catch (Exception e) {
            LOG.warn("Could not read XINFO STREAM for {}; treating as empty: {}", streamKey, e.getMessage());
            return "0-0";
        }
    }

    /** Extract {@code last-generated-id} from the alternating key/value {@code XINFO STREAM} list. */
    @VisibleForTesting
    static String extractLastGeneratedId(List<Object> info) {
        if (info == null) {
            return "0-0";
        }
        for (int i = 0; i + 1 < info.size(); i += 2) {
            Object key = info.get(i);
            if (key != null && "last-generated-id".equals(key.toString())) {
                Object val = info.get(i + 1);
                return val == null ? "0-0" : val.toString();
            }
        }
        return "0-0";
    }
}
