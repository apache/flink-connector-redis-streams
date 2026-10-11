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

package org.apache.flink.connector.redis.streams.sink;

import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.redis.streams.sink.config.JedisConfig;
import org.apache.flink.connector.redis.streams.sink.connection.JedisConnector;
import org.apache.flink.connector.redis.streams.sink.connection.JedisConnectorBuilder;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * A sink for publishing data into Redis.
 *
 * @param <T>
 */
public class RedisStreamsSink<T> extends AsyncSinkBase<T, RedisStreamsCommand> {

    private final JedisConfig jedisConfig;

    public RedisStreamsSink(
            JedisConfig jedisConfig,
            RedisStreamsCommandSerializer<T> converter,
            AsyncSinkWriterConfiguration asyncConfig) {
        super(
                converter,
                asyncConfig.getMaxBatchSize(),
                asyncConfig.getMaxInFlightRequests(),
                asyncConfig.getMaxBufferedRequests(),
                asyncConfig.getMaxBatchSizeInBytes(),
                asyncConfig.getMaxTimeInBufferMS(),
                asyncConfig.getMaxRecordSizeInBytes());
        this.jedisConfig = jedisConfig;
    }

    @Override
    public RedisStreamsWriter<T> createWriter(InitContext initContext) throws IOException {
        return restoreWriter(initContext, Collections.emptyList());
    }

    @Override
    public RedisStreamsWriter<T> restoreWriter(
            InitContext initContext,
            Collection<BufferedRequestState<RedisStreamsCommand>> recoveredState)
            throws IOException {
        AsyncSinkWriterConfiguration asyncConfig =
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(getMaxBatchSize())
                        .setMaxBatchSizeInBytes(getMaxBatchSizeInBytes())
                        .setMaxInFlightRequests(getMaxInFlightRequests())
                        .setMaxBufferedRequests(getMaxBufferedRequests())
                        .setMaxTimeInBufferMS(getMaxTimeInBufferMS())
                        .setMaxRecordSizeInBytes(getMaxRecordSizeInBytes())
                        .build();
        JedisConnector connection = JedisConnectorBuilder.build(jedisConfig);
        return new RedisStreamsWriter<>(
                connection, getElementConverter(), asyncConfig, initContext, recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<RedisStreamsCommand>>
            getWriterStateSerializer() {
        return new RedisStreamsStateSerializer();
    }
}
