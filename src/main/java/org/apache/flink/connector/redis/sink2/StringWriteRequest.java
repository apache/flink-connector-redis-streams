package org.apache.flink.connector.redis.sink2;

import redis.clients.jedis.UnifiedJedis;

public class StringWriteRequest implements RedisWriteRequest {

    private final String key;
    private final String value;

    public StringWriteRequest(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public void write(UnifiedJedis jedis) {
        jedis.set(key, value);
    }

    @Override
    public long getSizeInBytes() {
        return 18 + key.length() + value.length();
    }
}
