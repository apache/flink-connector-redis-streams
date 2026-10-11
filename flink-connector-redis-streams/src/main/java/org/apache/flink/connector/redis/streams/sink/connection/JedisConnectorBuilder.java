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

import org.apache.flink.connector.redis.streams.sink.config.JedisClusterConfig;
import org.apache.flink.connector.redis.streams.sink.config.JedisConfig;
import org.apache.flink.connector.redis.streams.sink.config.JedisPoolConfig;
import org.apache.flink.connector.redis.streams.sink.config.JedisSentinelConfig;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.util.Objects;

/** The builder for {@link JedisConnector}. */
public class JedisConnectorBuilder {

    /**
     * Initialize the {@link JedisConnector} based on the instance type.
     *
     * @param jedisConfig configuration base
     * @return @throws IllegalArgumentException if not valid configuration is provided
     */
    public static JedisConnector build(JedisConfig jedisConfig) {
        if (jedisConfig instanceof JedisPoolConfig) {
            JedisPoolConfig jedisPoolConfig = (JedisPoolConfig) jedisConfig;
            return JedisConnectorBuilder.build(jedisPoolConfig);
        } else if (jedisConfig instanceof JedisClusterConfig) {
            JedisClusterConfig jedisClusterConfig = (JedisClusterConfig) jedisConfig;
            return JedisConnectorBuilder.build(jedisClusterConfig);
        } else if (jedisConfig instanceof JedisSentinelConfig) {
            JedisSentinelConfig jedisSentinelConfig = (JedisSentinelConfig) jedisConfig;
            return JedisConnectorBuilder.build(jedisSentinelConfig);
        } else {
            throw new IllegalArgumentException("Jedis configuration not found");
        }
    }

    /**
     * Builds container for single Redis environment.
     *
     * @param jedisPoolConfig configuration for JedisPool
     * @return container for single Redis environment
     * @throws NullPointerException if jedisPoolConfig is null
     */
    public static JedisConnector build(JedisPoolConfig jedisPoolConfig) {
        Objects.requireNonNull(jedisPoolConfig, "Redis pool config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig =
                getGenericObjectPoolConfig(jedisPoolConfig);

        JedisPool jedisPool =
                new JedisPool(
                        genericObjectPoolConfig,
                        jedisPoolConfig.getHost(),
                        jedisPoolConfig.getPort(),
                        jedisPoolConfig.getConnectionTimeout(),
                        jedisPoolConfig.getPassword(),
                        jedisPoolConfig.getDatabase());
        return new JedisConnector(jedisPool);
    }

    /**
     * Builds container for Redis Cluster environment.
     *
     * @param jedisClusterConfig configuration for JedisCluster
     * @return container for Redis Cluster environment
     * @throws NullPointerException if jedisClusterConfig is null
     */
    public static JedisConnector build(JedisClusterConfig jedisClusterConfig) {
        Objects.requireNonNull(jedisClusterConfig, "Redis cluster config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig =
                getGenericObjectPoolConfig(jedisClusterConfig);

        JedisCluster jedisCluster =
                new JedisCluster(
                        jedisClusterConfig.getNodes(),
                        jedisClusterConfig.getConnectionTimeout(),
                        jedisClusterConfig.getConnectionTimeout(),
                        jedisClusterConfig.getMaxRedirections(),
                        jedisClusterConfig.getPassword(),
                        genericObjectPoolConfig);
        return new JedisConnector(jedisCluster);
    }

    /**
     * Builds container for Redis Sentinel environment.
     *
     * @param jedisSentinelConfig configuration for JedisSentinel
     * @return container for Redis sentinel environment
     * @throws NullPointerException if jedisSentinelConfig is null
     */
    public static JedisConnector build(JedisSentinelConfig jedisSentinelConfig) {
        Objects.requireNonNull(jedisSentinelConfig, "Redis sentinel config should not be Null");

        GenericObjectPoolConfig genericObjectPoolConfig =
                getGenericObjectPoolConfig(jedisSentinelConfig);

        JedisSentinelPool jedisSentinelPool =
                new JedisSentinelPool(
                        jedisSentinelConfig.getMasterName(),
                        jedisSentinelConfig.getSentinels(),
                        genericObjectPoolConfig,
                        jedisSentinelConfig.getConnectionTimeout(),
                        jedisSentinelConfig.getSoTimeout(),
                        jedisSentinelConfig.getPassword(),
                        jedisSentinelConfig.getDatabase());
        return new JedisConnector(jedisSentinelPool);
    }

    public static GenericObjectPoolConfig getGenericObjectPoolConfig(JedisConfig jedisConfig) {
        GenericObjectPoolConfig genericObjectPoolConfig =
                jedisConfig.getTestWhileIdle()
                        ? new redis.clients.jedis.JedisPoolConfig()
                        : new GenericObjectPoolConfig<>();
        genericObjectPoolConfig.setMaxIdle(jedisConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisConfig.getMinIdle());
        genericObjectPoolConfig.setTestOnBorrow(jedisConfig.getTestOnBorrow());
        genericObjectPoolConfig.setTestOnReturn(jedisConfig.getTestOnReturn());

        return genericObjectPoolConfig;
    }
}
