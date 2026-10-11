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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Converts a Redis Stream entry into a record of type {@code T}.
 *
 * <p>Returning {@code null} from {@link #deserialize} drops the record — the entry is still
 * XACKed so it does not re-appear in the PEL. Throwing {@link IOException} fails the task and
 * triggers a restart, replaying the entry via PEL recovery.
 *
 * <p>If the schema needs initialisation (opening a connection, reading a side-input, etc.) override
 * {@link #open(DeserializationSchema.InitializationContext)} — it is called exactly once after the
 * schema is deserialized on the TaskManager, before the first {@link #deserialize} call.
 */
@PublicEvolving
public interface RedisStreamsDeserializationSchema<T>
        extends Serializable, ResultTypeQueryable<T> {

    /**
     * Called once when the schema is instantiated on the TaskManager. Override to open resources,
     * connections, or side-inputs needed during deserialization.
     */
    default void open(DeserializationSchema.InitializationContext context) throws Exception {}

    /**
     * Deserializes a Redis Stream entry.
     *
     * @return the deserialized record, or {@code null} to drop the entry silently (still XACKed).
     * @throws IOException if deserialization fails; the task will restart and the entry will be
     *     re-delivered via PEL recovery.
     */
    T deserialize(String streamKey, String entryId, Map<String, String> fields) throws IOException;

    @Override
    TypeInformation<T> getProducedType();
}
