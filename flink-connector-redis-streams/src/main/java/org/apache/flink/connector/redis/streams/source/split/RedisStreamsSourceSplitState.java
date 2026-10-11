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

package org.apache.flink.connector.redis.streams.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

/**
 * Mutable per-split state. Tracks the latest emitted entry ID (for observability/restore) and the
 * IDs of records emitted since the last checkpoint barrier (for deferred XACK). All fields are
 * accessed exclusively from the task main thread — no synchronisation is required.
 */
@Internal
public class RedisStreamsSourceSplitState {

    private final RedisStreamsSourceSplit split;
    @Nullable private String currentEntryId;

    /**
     * IDs of records emitted via {@link
     * org.apache.flink.connector.redis.streams.source.reader.RedisStreamsRecordEmitter} since the
     * last call to {@link #drainDeferredAckIds()}. Populated on the task main thread; drained at
     * each checkpoint barrier to guarantee that only <em>emitted</em> records are XACKed after
     * checkpoint completion.
     */
    private final Deque<String> deferredAckIds = new ArrayDeque<>();

    public RedisStreamsSourceSplitState(RedisStreamsSourceSplit split) {
        this.split = Preconditions.checkNotNull(split, "split");
        // startingEntryId carries the last checkpointed position for observability;
        // server-side consumer-group state drives actual positioning via XREADGROUP >.
        this.currentEntryId = split.getStartingEntryId();
    }

    public String getStreamKey() {
        return split.getStreamKey();
    }

    @Nullable
    public String getCurrentEntryId() {
        return currentEntryId;
    }

    public void setCurrentEntryId(String currentEntryId) {
        this.currentEntryId = currentEntryId;
    }

    /**
     * Registers {@code id} as an emitted record that must be XACKed after the next checkpoint
     * completes. Called by the record emitter after each successful {@code output.collect()} —
     * including null/filtered records which still need to be removed from the Redis PEL.
     */
    public void addDeferredAckId(String id) {
        deferredAckIds.addLast(id);
    }

    /**
     * Returns all IDs accumulated since the last call and clears the internal queue. Called by
     * {@link org.apache.flink.connector.redis.streams.source.reader.RedisStreamsSourceReader} at
     * checkpoint barrier time to hand the emitted IDs over to the split reader for XACK.
     */
    public List<String> drainDeferredAckIds() {
        List<String> ids = new ArrayList<>(deferredAckIds);
        deferredAckIds.clear();
        return ids;
    }

    public RedisStreamsSourceSplit toSplit() {
        return new RedisStreamsSourceSplit(
                split.getStreamKey(), currentEntryId, split.getStoppingEntryId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RedisStreamsSourceSplitState that = (RedisStreamsSourceSplitState) o;
        return split.equals(that.split) && Objects.equals(currentEntryId, that.currentEntryId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(split, currentEntryId);
    }

    @Override
    public String toString() {
        return "RedisStreamsSourceSplitState{"
                + "split="
                + split
                + ", currentEntryId='"
                + currentEntryId
                + '\''
                + '}';
    }
}
