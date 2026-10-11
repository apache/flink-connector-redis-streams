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

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The Redis implementation for {@link SimpleVersionedSerializer}. */
public class RedisStreamsStateSerializer
        implements SimpleVersionedSerializer<BufferedRequestState<RedisStreamsCommand>> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(BufferedRequestState<RedisStreamsCommand> obj) throws IOException {
        Collection<RequestEntryWrapper<RedisStreamsCommand>> bufferState =
                obj.getBufferedRequestEntries();

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {

            out.writeInt(getVersion());
            out.writeInt(bufferState.size());

            for (RequestEntryWrapper<RedisStreamsCommand> wrapper : bufferState) {
                RedisStreamsCommand command = wrapper.getRequestEntry();
                writeString(out, command.key);
                out.writeInt(command.value.size());
                for (Map.Entry<String, String> entry : command.value.entrySet()) {
                    writeString(out, entry.getKey());
                    writeString(out, entry.getValue());
                }
            }

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public BufferedRequestState<RedisStreamsCommand> deserialize(int version, byte[] serialized)
            throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {

            int byteVersion = in.readInt();

            int bufferSize = in.readInt();
            List<RequestEntryWrapper<RedisStreamsCommand>> state = new ArrayList<>();
            for (int bs = 0; bs < bufferSize; bs++) {
                String key = readString(in);

                int valueSize = in.readInt();
                Map<String, String> values = new HashMap<>();
                for (int i = 0; i < valueSize; i++) {
                    String eKey = readString(in);
                    String eValue = readString(in);
                    values.put(eKey, eValue);
                }

                RedisStreamsCommand command =
                        RedisStreamsCommand.builder().withKey(key).withValue(values).build();

                state.add(new RequestEntryWrapper<>(command, command.getMessageSize()));
            }
            return new BufferedRequestState<>(state);
        }
    }

    private void writeString(final DataOutputStream out, String value) throws IOException {
        out.writeInt(value.length());
        out.writeBytes(value);
    }

    private String readString(final DataInputStream in) throws IOException {
        int sizeToRead = in.readInt();
        byte[] bytesRead = new byte[sizeToRead];
        int sizeRead = in.read(bytesRead);

        if (sizeToRead != sizeRead) {
            throw new IOException(
                    String.format("Expected to read %s but read %s", sizeToRead, sizeRead));
        }

        return new String(bytesRead);
    }
}
