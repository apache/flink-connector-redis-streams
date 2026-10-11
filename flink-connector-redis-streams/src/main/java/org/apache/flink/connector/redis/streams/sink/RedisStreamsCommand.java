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

import org.apache.flink.connector.redis.streams.sink.connection.JedisConnector;

import redis.clients.jedis.StreamEntryID;

import java.io.Serializable;
import java.util.Map;

/** A Redis Streams Command. */
public class RedisStreamsCommand implements Serializable {

    private transient StreamEntryID streamId = null;
    public final String key;
    public final Map<String, String> value;

    private RedisStreamsCommand(String key, Map<String, String> value) {
        this.key = key;
        this.value = value;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void send(JedisConnector connector) {
        this.streamId =
                connector
                        .getJedisCommands()
                        .xadd(
                                key,
                                (this.streamId != null) ? this.streamId : StreamEntryID.NEW_ENTRY,
                                value);
    }

    public boolean sendCorrectly() {
        return true;
    }

    public boolean sendIncorrectly() {
        return !sendCorrectly();
    }

    public long getMessageSize() {
        return this.key.length()
                + this.value.entrySet().stream()
                        .map(k -> k.getKey().length() + k.getValue().length())
                        .reduce(Integer::sum)
                        .orElse(0);
    }

    /** The builder for {@link RedisStreamsCommand}. */
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

        public RedisStreamsCommand build() {
            return new RedisStreamsCommand(key, value);
        }
    }
}
