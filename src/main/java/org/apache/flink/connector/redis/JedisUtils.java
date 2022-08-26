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

import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.UnifiedJedis;

import java.util.Properties;

/** */
public class JedisUtils {

    public static UnifiedJedis createJedis(Properties configProps) {

        String host =
                configProps.getProperty(
                        JedisConfigConstants.REDIS_HOST, JedisConfigConstants.DEFAULT_REDIS_HOST);

        int port =
                Integer.parseInt(
                        configProps.getProperty(
                                JedisConfigConstants.REDIS_PORT,
                                Integer.toString(JedisConfigConstants.DEFAULT_REDIS_PORT)));

        return new UnifiedJedis(new HostAndPort(host, port));
    }

    public static Connection createConnection(Properties configProps) {

        String host =
                configProps.getProperty(
                        JedisConfigConstants.REDIS_HOST, JedisConfigConstants.DEFAULT_REDIS_HOST);

        int port =
                Integer.parseInt(
                        configProps.getProperty(
                                JedisConfigConstants.REDIS_PORT,
                                Integer.toString(JedisConfigConstants.DEFAULT_REDIS_PORT)));

        return new Connection(host, port);
    }
}
