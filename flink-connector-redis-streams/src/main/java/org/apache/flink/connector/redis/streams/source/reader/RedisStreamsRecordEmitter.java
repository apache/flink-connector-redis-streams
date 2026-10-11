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
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.redis.streams.source.RedisStreamsDeserializationSchema;
import org.apache.flink.connector.redis.streams.source.split.RedisStreamsSourceSplitState;

import io.lettuce.core.StreamMessage;

import java.io.IOException;

/**
 * Record emitter that deserializes Redis StreamMessages.
 *
 * <p>After each record (including null/filtered ones), the entry ID is registered in the split
 * state's deferred-ACK queue. This ensures that only records that have actually been emitted past
 * the checkpoint barrier are XACKed after checkpoint completion — preventing silent data loss on
 * crash recovery.
 */
@Internal
public class RedisStreamsRecordEmitter<T>
        implements RecordEmitter<StreamMessage<String, String>, T, RedisStreamsSourceSplitState> {

    private final RedisStreamsDeserializationSchema<T> deserializationSchema;

    public RedisStreamsRecordEmitter(RedisStreamsDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void emitRecord(
            StreamMessage<String, String> element,
            SourceOutput<T> output,
            RedisStreamsSourceSplitState splitState)
            throws Exception {
        try {
            T record =
                    deserializationSchema.deserialize(
                            element.getStream(), element.getId(), element.getBody());
            splitState.setCurrentEntryId(element.getId());
            // Register BEFORE collecting: if collect() throws the entry ID is still in the
            // deferred queue and will be re-delivered on PEL recovery rather than orphaned.
            // Null records are filter results — they still need to be XACKed to drain the PEL.
            splitState.addDeferredAckId(element.getId());
            if (record != null) {
                output.collect(record);
            }
        } catch (Exception e) {
            throw new IOException(
                    "Failed to deserialize record from stream "
                            + element.getStream()
                            + " id "
                            + element.getId(),
                    e);
        }
    }
}
