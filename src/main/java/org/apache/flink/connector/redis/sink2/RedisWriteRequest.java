package org.apache.flink.connector.redis.sink2;

import redis.clients.jedis.UnifiedJedis;

import java.io.Serializable;

public interface RedisWriteRequest extends Serializable {
    void write(UnifiedJedis jedis);

    long getSizeInBytes();
}
