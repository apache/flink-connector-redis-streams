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
package org.apache.flink.connector.redis.streams.sink.command;


import org.apache.flink.connector.redis.streams.sink.connection.JedisConnector;
import redis.clients.jedis.StreamEntryID;

import java.util.Map;

public class StreamRedisCommand implements RedisCommand {
    public final String key;
    public final Map<String, String> value;

    private StreamRedisCommand(String key, Map<String, String> value) {
        this.key = key;
        this.value = value;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void send(JedisConnector connector) {
        connector.getJedisCommands().xadd(key, StreamEntryID.NEW_ENTRY, value);
    }

    public static class Builder {
        private String key;
        private Map<String, String> value;

        public Builder withKey(String key) {
            this.key = key;
            return this;
        }

        public Builder withValue(Map<String, String> value) {
            this.value = value;
            return this;
        }

        public StreamRedisCommand build() {
            return new StreamRedisCommand(key, value);
        }
    }
}