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

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
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

/** A redis container for testing. */
@Testcontainers
public class BaseITCase {

    @Container
    private GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7.0.5-alpine"))
                    .withExposedPorts(6379);

    protected Jedis jedis;

    public static MiniClusterWithClientResource cluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @BeforeAll
    public static void beforeAll() throws Exception {
        cluster.before();
    }

    @AfterAll
    public static void afterAll() {
        cluster.after();
    }

    @BeforeEach
    public void setUp() {
        jedis = new Jedis(redisHost(), redisPort());
    }

    @AfterEach
    public void cleanUp() {
        jedis.close();
    }

    public String redisHost() {
        return redis.getHost();
    }

    public Integer redisPort() {
        return redis.getFirstMappedPort();
    }
}
