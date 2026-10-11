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

package org.apache.flink.connector.redis.streams.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.redis.streams.source.config.RedisStreamsSourceConfig;
import org.apache.flink.connector.redis.streams.source.config.StartupMode;
import org.apache.flink.connector.redis.streams.source.enumerator.RedisStreamsSourceEnumerator;
import org.apache.flink.connector.redis.streams.source.enumerator.RedisStreamsSourceEnumeratorState;
import org.apache.flink.connector.redis.streams.source.enumerator.RedisStreamsSourceEnumeratorStateSerializer;
import org.apache.flink.connector.redis.streams.source.reader.RedisStreamsSourceReader;
import org.apache.flink.connector.redis.streams.source.reader.split.RedisStreamsSplitReader;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplit;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import io.lettuce.core.StreamMessage;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Flink Source for Redis Streams. Provides at-least-once delivery via Redis consumer groups and
 * checkpoint-aligned XACK.
 *
 * <pre>{@code
 * RedisStreamsSource<Map<String, String>> source =
 *     RedisStreamsSource.<Map<String, String>>builder()
 *         .setHost("localhost").setPort(6379)
 *         .setStreamKeys(List.of("my-stream"))
 *         .setConsumerGroup("flink-consumer-group")
 *         .setDeserializationSchema(new SimpleMapDeserializationSchema())
 *         .build();
 * }</pre>
 */
@PublicEvolving
public class RedisStreamsSource<T>
        implements Source<T, RedisStreamsSourceSplit, RedisStreamsSourceEnumeratorState>,
                ResultTypeQueryable<T>,
                Serializable {

    private static final long serialVersionUID = 1L;

    private final RedisStreamsSourceConfig sourceConfig;
    private final RedisStreamsDeserializationSchema<T> deserializationSchema;

    public RedisStreamsSource(
            RedisStreamsSourceConfig sourceConfig,
            RedisStreamsDeserializationSchema<T> deserializationSchema) {
        this.sourceConfig = Preconditions.checkNotNull(sourceConfig);
        this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema);
    }

    @Override
    public Boundedness getBoundedness() {
        return sourceConfig.isBounded() ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, RedisStreamsSourceSplit> createReader(
            SourceReaderContext readerContext) {
        AtomicReference<RedisStreamsSplitReader> splitReaderRef = new AtomicReference<>();

        SingleThreadFetcherManager<StreamMessage<String, String>, RedisStreamsSourceSplit>
                splitFetcherManager =
                        new SingleThreadFetcherManager<>(
                                () -> {
                                    RedisStreamsSplitReader reader =
                                            new RedisStreamsSplitReader(
                                                    sourceConfig,
                                                    readerContext.getIndexOfSubtask());
                                    splitReaderRef.set(reader);
                                    return reader;
                                },
                                readerContext.getConfiguration());

        return new RedisStreamsSourceReader<>(
                splitFetcherManager,
                deserializationSchema,
                splitReaderRef,
                readerContext.getConfiguration(),
                readerContext);
    }

    @Override
    public SplitEnumerator<RedisStreamsSourceSplit, RedisStreamsSourceEnumeratorState>
            createEnumerator(SplitEnumeratorContext<RedisStreamsSourceSplit> enumContext) {
        return new RedisStreamsSourceEnumerator(sourceConfig, enumContext, null);
    }

    @Override
    public SplitEnumerator<RedisStreamsSourceSplit, RedisStreamsSourceEnumeratorState>
            restoreEnumerator(
                    SplitEnumeratorContext<RedisStreamsSourceSplit> enumContext,
                    RedisStreamsSourceEnumeratorState checkpoint) {
        return new RedisStreamsSourceEnumerator(sourceConfig, enumContext, checkpoint);
    }

    @Override
    @Internal
    public SimpleVersionedSerializer<RedisStreamsSourceSplit> getSplitSerializer() {
        return RedisStreamsSourceSplitSerializer.INSTANCE;
    }

    @Override
    @Internal
    public SimpleVersionedSerializer<RedisStreamsSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return RedisStreamsSourceEnumeratorStateSerializer.INSTANCE;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    public static <T> RedisStreamsSourceBuilder<T> builder() {
        return new RedisStreamsSourceBuilder<>();
    }

    /** Builder for {@link RedisStreamsSource}. */
    @PublicEvolving
    public static class RedisStreamsSourceBuilder<T> {
        private final RedisStreamsSourceConfig.Builder configBuilder =
                RedisStreamsSourceConfig.builder();
        private RedisStreamsDeserializationSchema<T> deserializationSchema;

        public RedisStreamsSourceBuilder<T> setHost(String host) {
            configBuilder.setHost(host);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setPort(int port) {
            configBuilder.setPort(port);
            return this;
        }

        /** Activate cluster mode with one or more {@code host:port} seeds. */
        public RedisStreamsSourceBuilder<T> setClusterNodes(List<String> clusterNodes) {
            configBuilder.setClusterNodes(clusterNodes);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setClusterTopologyRefreshPeriodMs(long periodMs) {
            configBuilder.setClusterTopologyRefreshPeriodMs(periodMs);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setPassword(String password) {
            configBuilder.setPassword(password);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setDatabase(int database) {
            configBuilder.setDatabase(database);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setStreamKeys(List<String> streamKeys) {
            configBuilder.setStreamKeys(streamKeys);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setConsumerGroup(String consumerGroup) {
            configBuilder.setConsumerGroup(consumerGroup);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setConsumerName(String consumerName) {
            configBuilder.setConsumerName(consumerName);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setBounded(boolean bounded) {
            configBuilder.setBounded(bounded);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setPollTimeout(long pollTimeout) {
            configBuilder.setPollTimeout(pollTimeout);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setBatchSize(int batchSize) {
            configBuilder.setBatchSize(batchSize);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setStartupMode(StartupMode startupMode) {
            configBuilder.setStartupMode(startupMode);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setMaxDeferredAckQueueSize(
                int maxDeferredAckQueueSize) {
            configBuilder.setMaxDeferredAckQueueSize(maxDeferredAckQueueSize);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setCircuitBreakerOpenDurationMs(
                long circuitBreakerOpenDurationMs) {
            configBuilder.setCircuitBreakerOpenDurationMs(circuitBreakerOpenDurationMs);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setCircuitBreakerFailureThreshold(
                int circuitBreakerFailureThreshold) {
            configBuilder.setCircuitBreakerFailureThreshold(circuitBreakerFailureThreshold);
            return this;
        }

        public RedisStreamsSourceBuilder<T> setDeserializationSchema(
                RedisStreamsDeserializationSchema<T> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
            return this;
        }

        public RedisStreamsSource<T> build() {
            Preconditions.checkNotNull(deserializationSchema, "Deserialization schema must be set");
            return new RedisStreamsSource<>(configBuilder.build(), deserializationSchema);
        }
    }
}
