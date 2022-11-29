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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.redis.streams.sink.command.RedisCommand;
import org.apache.flink.connector.redis.streams.sink.connection.JedisConnector;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

public class RedisStreamsWriter<T> implements SinkWriter<T> {

    private final JedisConnector jedisConnector;
    private final RedisSerializer<T> serializer;
    private final Queue<RedisCommand> queue = new ArrayDeque<>();

    public RedisStreamsWriter(JedisConnector jedisConnector, RedisSerializer<T> serializer) {
        this.jedisConnector = jedisConnector;
        this.serializer = serializer;
    }


    @Override
    public void write(T input, Context context) throws IOException, InterruptedException {
        RedisCommand message = serializer.getMessage(input);
        queue.add(message);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        flush();
    }

    private void flush() {
        while(!this.queue.isEmpty()) {
            RedisCommand element = this.queue.remove();
            element.send(this.jedisConnector);
        }
    }

    @Override
    public void close() throws Exception {
        jedisConnector.close();
    }
}
