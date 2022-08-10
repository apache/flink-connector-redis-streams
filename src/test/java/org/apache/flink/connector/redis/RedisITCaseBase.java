/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.redis;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.UnifiedJedis;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/** */
@Testcontainers
public abstract class RedisITCaseBase extends AbstractTestBase {

    public static final String REDIS_IMAGE = "redis";
    private static final int REDIS_PORT = 6379;

    private static final AtomicBoolean running = new AtomicBoolean(false);

    @Container
    private static final GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse(REDIS_IMAGE)).withExposedPorts(REDIS_PORT);

    public static MiniClusterWithClientResource cluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());
    protected UnifiedJedis jedis;

    @BeforeAll
    public static void beforeAll() throws Exception {
        cluster.before();
    }

    @AfterAll
    public static void afterAll() {
        cluster.after();
    }

    protected static synchronized void start() {
        if (!running.get()) {
            redis.start();
            running.set(true);
        }
    }

    protected static void stop() {
        redis.stop();
        running.set(false);
    }

    @BeforeEach
    public void setUp() {
        Properties config = getConfigProperties();
        try (Jedis j =
                new Jedis(
                        config.getProperty(JedisConfigConstants.REDIS_HOST),
                        Integer.parseInt(config.getProperty(JedisConfigConstants.REDIS_PORT)))) {
            j.flushAll();
        }

        jedis = JedisUtils.createJedis(config);
    }

    @AfterEach
    public void tearDown() {
        if (jedis != null) {
            jedis.close();
        }
    }

    protected Properties getConfigProperties() {
        start();

        Properties configProps = new Properties();
        // configProps.setProperty(RedisConfigConstants.REDIS_HOST, redis.getContainerIpAddress());
        configProps.setProperty(JedisConfigConstants.REDIS_HOST, redis.getHost());
        configProps.setProperty(
                JedisConfigConstants.REDIS_PORT, Integer.toString(redis.getFirstMappedPort()));
        return configProps;
    }
}
