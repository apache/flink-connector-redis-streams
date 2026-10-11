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

package org.apache.flink.connector.redis.streams.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.redis.streams.source.RedisStreamsDeserializationSchema;
import org.apache.flink.connector.redis.streams.source.reader.split.RedisStreamsSplitReader;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplit;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplitState;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.UserCodeClassLoader;

import io.lettuce.core.StreamMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Source reader for Redis Streams.
 *
 * <p>The reader owns the deferred-XACK lifecycle:
 *
 * <ol>
 *   <li>{@link RedisStreamsRecordEmitter} registers each emitted entry ID in the split state's
 *       deferred-ACK queue (task main thread).
 *   <li>{@link #snapshotState} drains those IDs from all active split states and hands them to
 *       {@link RedisStreamsSplitReader#markCheckpoint} — only IDs that crossed the barrier are
 *       eligible for XACK.
 *   <li>{@link #notifyCheckpointComplete} triggers the actual XACK.
 *   <li>{@link #notifyCheckpointAborted} discards the snapshot so back-pressure is released.
 * </ol>
 */
@Internal
public class RedisStreamsSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                StreamMessage<String, String>,
                T,
                RedisStreamsSourceSplit,
                RedisStreamsSourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisStreamsSourceReader.class);

    private final RedisStreamsDeserializationSchema<T> deserializationSchema;
    private final SourceReaderContext context;
    private final AtomicReference<RedisStreamsSplitReader> splitReaderRef;

    /**
     * Tracks all active split states so {@link #snapshotState} can drain their deferred-ACK queues
     * without accessing {@code SourceReaderBase}'s private {@code splitStates} field. Maintained
     * exclusively on the task main thread — no synchronisation required.
     */
    private final Map<String, RedisStreamsSourceSplitState> activeSplitStates = new HashMap<>();

    public RedisStreamsSourceReader(
            SingleThreadFetcherManager<StreamMessage<String, String>, RedisStreamsSourceSplit>
                    splitFetcherManager,
            RedisStreamsDeserializationSchema<T> deserializationSchema,
            AtomicReference<RedisStreamsSplitReader> splitReaderRef,
            Configuration config,
            SourceReaderContext context) {
        super(
                splitFetcherManager,
                new RedisStreamsRecordEmitter<>(deserializationSchema),
                config,
                context);
        this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema);
        this.context = context;
        this.splitReaderRef = Preconditions.checkNotNull(splitReaderRef);

        LOG.info(
                "RedisStreamsSourceReader initialized for subtask {}", context.getIndexOfSubtask());
    }

    @Override
    public void start() {
        try {
            deserializationSchema.open(
                    new DeserializationSchema.InitializationContext() {
                        @Override
                        public MetricGroup getMetricGroup() {
                            return context.metricGroup().addGroup("deserializer");
                        }

                        @Override
                        public UserCodeClassLoader getUserCodeClassLoader() {
                            return context.getUserCodeClassLoader();
                        }
                    });
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open deserialization schema", e);
        }
        super.start();
    }

    @Override
    protected void onSplitFinished(Map<String, RedisStreamsSourceSplitState> finishedSplitIds) {
        LOG.info("Splits finished: {}", finishedSplitIds.keySet());
        activeSplitStates.keySet().removeAll(finishedSplitIds.keySet());
    }

    @Override
    protected RedisStreamsSourceSplitState initializedState(RedisStreamsSourceSplit split) {
        RedisStreamsSourceSplitState state = new RedisStreamsSourceSplitState(split);
        activeSplitStates.put(split.splitId(), state);
        return state;
    }

    @Override
    protected RedisStreamsSourceSplit toSplitType(
            String splitId, RedisStreamsSourceSplitState splitState) {
        return splitState.toSplit();
    }

    @Override
    public List<RedisStreamsSourceSplit> snapshotState(long checkpointId) {
        List<RedisStreamsSourceSplit> splits = super.snapshotState(checkpointId);
        RedisStreamsSplitReader reader = splitReaderRef.get();
        if (reader != null) {
            // Drain only EMITTED entry IDs from split states. Records that were fetched but are
            // still in the internal buffer (not yet through RecordEmitter) are NOT included —
            // this is the key correctness guarantee: we only XACK what was durably emitted
            // before the checkpoint barrier.
            Map<String, List<String>> emittedIdsBySplit = new HashMap<>();
            for (Map.Entry<String, RedisStreamsSourceSplitState> e : activeSplitStates.entrySet()) {
                List<String> ids = e.getValue().drainDeferredAckIds();
                if (!ids.isEmpty()) {
                    emittedIdsBySplit.put(e.getKey(), ids);
                }
            }
            reader.markCheckpoint(checkpointId, emittedIdsBySplit);
        }
        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        RedisStreamsSplitReader reader = splitReaderRef.get();
        if (reader != null) {
            reader.acknowledgeAllPendingMessagesAtCheckpoint(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
        RedisStreamsSplitReader reader = splitReaderRef.get();
        if (reader != null) {
            reader.discardCheckpoint(checkpointId);
        }
    }
}
