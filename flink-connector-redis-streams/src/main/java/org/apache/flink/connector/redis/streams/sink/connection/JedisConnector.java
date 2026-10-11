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

package org.apache.flink.connector.redis.streams.sink.connection;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.commands.JedisCommands;

import java.io.Serializable;

/** A connector to Redis. */
public class JedisConnector implements AutoCloseable, Serializable {

    private transient JedisCluster jedisCluster;
    private transient JedisPool jedisPool;
    private transient JedisSentinelPool jedisSentinelPool;

    public JedisConnector(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    public JedisConnector(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public JedisConnector(JedisSentinelPool jedisSentinelPool) {
        this.jedisSentinelPool = jedisSentinelPool;
    }

    public JedisCommands getJedisCommands() {
        if (jedisCluster != null) {
            return jedisCluster;
        }
        if (jedisPool != null) {
            return jedisPool.getResource();
        }
        if (jedisSentinelPool != null) {
            return jedisSentinelPool.getResource();
        }

        throw new IllegalArgumentException("No redis connection found");
    }

    @Override
    public void close() {
        if (jedisCluster != null) {
            jedisCluster.close();
        }
        if (jedisPool != null) {
            jedisPool.close();
        }
        if (jedisSentinelPool != null) {
            jedisSentinelPool.close();
        }
    }
}
