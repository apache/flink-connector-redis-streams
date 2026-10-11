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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Binary serializer for {@link RedisStreamsSourceEnumeratorState}.
 *
 * <p>Format (v1): {@code pendingCount (int) | pendingKeys (UTF…) | stoppingCount (int) |
 * stoppingEntries (UTF key, UTF value)…}.
 *
 * <p>When the wire format changes in future releases, bump {@link #CURRENT_VERSION} and add a
 * migration case to {@link #deserialize(int, byte[])}.
 */
@Internal
public class RedisStreamsSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<RedisStreamsSourceEnumeratorState> {

    public static final RedisStreamsSourceEnumeratorStateSerializer INSTANCE =
            new RedisStreamsSourceEnumeratorStateSerializer();

    /** Current serialization format version. Bump when the binary layout changes post-release. */
    private static final int CURRENT_VERSION = 1;

    private static final int SERIALIZER_INITIAL_CAPACITY = 512;

    private RedisStreamsSourceEnumeratorStateSerializer() {}

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RedisStreamsSourceEnumeratorState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(SERIALIZER_INITIAL_CAPACITY);
        Set<String> pendingSplits = state.getPendingSplits();
        out.writeInt(pendingSplits.size());
        for (String split : pendingSplits) {
            out.writeUTF(split);
        }
        Map<String, String> stoppingEntryIds = state.getStoppingEntryIds();
        out.writeInt(stoppingEntryIds.size());
        for (Map.Entry<String, String> e : stoppingEntryIds.entrySet()) {
            out.writeUTF(e.getKey());
            out.writeUTF(e.getValue()); // null values guarded by EnumeratorState constructor
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public RedisStreamsSourceEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException(
                    "Unsupported serializer version: "
                            + version
                            + ". This connector has not been released yet; "
                            + "please start fresh (no migration path from development snapshots).");
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        int pendingSize = in.readInt();
        Set<String> pendingSplits = new HashSet<>(pendingSize);
        for (int i = 0; i < pendingSize; i++) {
            pendingSplits.add(in.readUTF());
        }

        int stoppingSize = in.readInt();
        Map<String, String> stoppingEntryIds = new HashMap<>(stoppingSize);
        for (int i = 0; i < stoppingSize; i++) {
            String key = in.readUTF();
            String value = in.readUTF();
            stoppingEntryIds.put(key, value);
        }
        return new RedisStreamsSourceEnumeratorState(pendingSplits, stoppingEntryIds);
    }
}
