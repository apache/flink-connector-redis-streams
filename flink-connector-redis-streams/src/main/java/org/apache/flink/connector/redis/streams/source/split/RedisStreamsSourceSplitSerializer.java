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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/**
 * Binary serializer for {@link RedisStreamsSourceSplit}.
 *
 * <p>Format (v1): {@code streamKey (UTF) | startingEntryId (nullable UTF) | stoppingEntryId
 * (nullable UTF)}. Each nullable field is written as a boolean presence flag followed by the UTF
 * string when present.
 *
 * <p>When the wire format changes in future releases, bump {@link #CURRENT_VERSION} and add a
 * migration case to {@link #deserialize(int, byte[])}.
 */
@Internal
public final class RedisStreamsSourceSplitSerializer
        implements SimpleVersionedSerializer<RedisStreamsSourceSplit> {

    public static final RedisStreamsSourceSplitSerializer INSTANCE =
            new RedisStreamsSourceSplitSerializer();

    /** Current serialization format version. Bump when the binary layout changes post-release. */
    private static final int CURRENT_VERSION = 1;

    private static final int SERIALIZER_INITIAL_CAPACITY = 256;

    private RedisStreamsSourceSplitSerializer() {}

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RedisStreamsSourceSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(SERIALIZER_INITIAL_CAPACITY);
        out.writeUTF(split.getStreamKey());
        writeNullable(out, split.getStartingEntryId());
        writeNullable(out, split.getStoppingEntryId());
        return out.getCopyOfBuffer();
    }

    @Override
    public RedisStreamsSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException(
                    "Unsupported serializer version: "
                            + version
                            + ". This connector has not been released yet; "
                            + "please start fresh (no migration path from development snapshots).");
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        String streamKey = in.readUTF();
        String startingEntryId = readNullable(in);
        String stoppingEntryId = readNullable(in);
        return new RedisStreamsSourceSplit(streamKey, startingEntryId, stoppingEntryId);
    }

    private static void writeNullable(DataOutputSerializer out, String value) throws IOException {
        if (value == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(value);
        }
    }

    private static String readNullable(DataInputDeserializer in) throws IOException {
        return in.readBoolean() ? in.readUTF() : null;
    }
}
