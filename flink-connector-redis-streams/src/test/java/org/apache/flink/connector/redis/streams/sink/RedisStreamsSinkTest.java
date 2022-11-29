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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.redis.streams.sink.command.StreamRedisCommand;
import org.apache.flink.connector.redis.streams.sink.config.JedisPoolConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RedisStreamsSinkTest extends BaseITCase {
    @Test
    public void testStreamCommand() throws Exception {

        JedisPoolConfig jedisConfig = new JedisPoolConfig.Builder()
                .setHost(redisHost())
                .setPort(redisPort())
                .build();

        RedisSerializer<Tuple3<String,String,String>> serializer = input -> {
            Map<String, String> value = new HashMap<>();
            value.put(input.f1, input.f2);
            return StreamRedisCommand.builder()
                    .withKey(input.f0)
                    .withValue(value)
                    .build();
        };

        RedisStreamsSink<Tuple3<String,String,String>> underTest = new RedisStreamsSink<>(jedisConfig, serializer);

        List<Tuple3<String,String,String>> source = Arrays.asList(
                Tuple3.of("one", "onekey", "onevalue"),
                Tuple3.of("two", "firstkey", "firstvalue"),
                Tuple3.of("two", "secontkey", "secondvalue"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.fromCollection(source).sinkTo(underTest);
        env.execute();

        // verify results
        assertEquals(1, jedis.xlen("one"));
        assertEquals(2, jedis.xlen("two"));

    }
}