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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RedisStreamsStateSerializerTest {

    @Test
    void testSerDe() throws IOException {

        RedisStreamsCommand command1 =
                RedisStreamsCommand.builder()
                        .withKey("test1")
                        .withValue(
                                new HashMap<String, String>() {
                                    {
                                        put("first", "1");
                                        put("second", "2");
                                    }
                                })
                        .build();

        RedisStreamsCommand command2 =
                RedisStreamsCommand.builder()
                        .withKey("test2")
                        .withValue(
                                new HashMap<String, String>() {
                                    {
                                        put("third", "3");
                                        put("fourth", "4");
                                    }
                                })
                        .build();

        List<RequestEntryWrapper<RedisStreamsCommand>> state = new ArrayList<>();
        state.add(new RequestEntryWrapper<>(command1, command1.getMessageSize()));
        state.add(new RequestEntryWrapper<>(command2, command2.getMessageSize()));

        RedisStreamsStateSerializer serializer = new RedisStreamsStateSerializer();

        byte[] serialized = serializer.serialize(new BufferedRequestState<>(state));
        BufferedRequestState<RedisStreamsCommand> deserialized =
                serializer.deserialize(1, serialized);
        assertEquals(2, deserialized.getBufferedRequestEntries().size());

        RedisStreamsCommand newCommand =
                deserialized.getBufferedRequestEntries().get(0).getRequestEntry();
        assertEquals(command1.key, newCommand.key);
        assertEquals(command1.value, newCommand.value);

        newCommand = deserialized.getBufferedRequestEntries().get(1).getRequestEntry();
        assertEquals(command2.key, newCommand.key);
        assertEquals(command2.value, newCommand.value);
    }
}
