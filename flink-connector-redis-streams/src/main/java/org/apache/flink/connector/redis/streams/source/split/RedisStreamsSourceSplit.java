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
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Immutable Redis Stream split.
 *
 * <p>A split is identified solely by its {@link #streamKey} — at most one split exists per stream
 * key, and at most one subtask reads any given stream. This guarantees Redis Streams' total
 * within-stream ordering is preserved end-to-end.
 *
 * <p>{@link #startingEntryId} carries the last checkpointed position for observability and
 * initialises {@code RedisStreamsSourceSplitState.currentEntryId} on restore, but does <em>not</em>
 * drive actual read positioning — that is delegated to the Redis consumer-group server-side state
 * via {@code XREADGROUP ... STREAMS key >}.
 *
 * <p>{@link #stoppingEntryId} is non-null only in bounded mode; it is frozen at job startup via
 * {@code XINFO STREAM} and persisted through the enumerator checkpoint so that re-partitioned jobs
 * terminate at the same logical boundary.
 */
@Internal
public class RedisStreamsSourceSplit implements SourceSplit {

    /**
     * Redis stream key — the identity of this split. {@link #splitId()} delegates to this field.
     */
    private final String streamKey;

    /**
     * Last checkpointed entry ID, or {@code null} on first assignment. Stored for observability
     * ({@code currentEntryId} in the split state) and to persist the advancing read position
     * across checkpoints. The split reader ignores this field for positioning; Redis consumer-group
     * state is authoritative.
     */
    @Nullable private final String startingEntryId;

    /**
     * Inclusive upper-bound entry ID in bounded mode ({@code setBounded(true)}), or {@code null}
     * for unbounded streaming. Frozen once at enumerator startup via {@code XINFO STREAM} and
     * persisted in the enumerator checkpoint.
     */
    @Nullable private final String stoppingEntryId;

    public RedisStreamsSourceSplit(
            String streamKey, @Nullable String startingEntryId, @Nullable String stoppingEntryId) {
        this.streamKey = Preconditions.checkNotNull(streamKey, "streamKey");
        validateEntryId(startingEntryId, "startingEntryId");
        validateEntryId(stoppingEntryId, "stoppingEntryId");
        this.startingEntryId = startingEntryId;
        this.stoppingEntryId = stoppingEntryId;
    }

    public RedisStreamsSourceSplit(String streamKey) {
        this(streamKey, null, null);
    }

    /**
     * Validates that {@code id} is either {@code null} or a valid Redis entry ID of the form
     * {@code <millis>-<seq>} (e.g. {@code 1700000000000-0}) or a special sentinel ({@code 0-0},
     * {@code $}). Garbage values would otherwise surface as a {@link NumberFormatException} deep
     * inside {@link
     * org.apache.flink.connector.redis.streams.source.reader.split.RedisStreamsSplitReader#compareEntryIds}.
     */
    private static void validateEntryId(@Nullable String id, String fieldName) {
        if (id == null || id.equals("$") || id.equals("0-0")) {
            return;
        }
        int dash = id.indexOf('-');
        boolean valid = dash > 0 && dash < id.length() - 1;
        if (valid) {
            try {
                Long.parseLong(id.substring(0, dash));
                Long.parseLong(id.substring(dash + 1));
                return;
            } catch (NumberFormatException ignored) {
                valid = false;
            }
        }
        if (!valid) {
            throw new IllegalArgumentException(
                    fieldName
                            + " must be a valid Redis entry ID (millis-seq, e.g. 1700000000000-0),"
                            + " '0-0', '$', or null; got: "
                            + id);
        }
    }

    @Override
    public String splitId() {
        return streamKey;
    }

    public String getStreamKey() {
        return streamKey;
    }

    @Nullable
    public String getStartingEntryId() {
        return startingEntryId;
    }

    @Nullable
    public String getStoppingEntryId() {
        return stoppingEntryId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RedisStreamsSourceSplit that = (RedisStreamsSourceSplit) o;
        return streamKey.equals(that.streamKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamKey);
    }

    @Override
    public String toString() {
        return "RedisStreamsSourceSplit{"
                + "streamKey='"
                + streamKey
                + '\''
                + ", startingEntryId="
                + startingEntryId
                + ", stoppingEntryId="
                + stoppingEntryId
                + '}';
    }
}
