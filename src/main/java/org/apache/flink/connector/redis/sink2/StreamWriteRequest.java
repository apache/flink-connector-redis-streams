package org.apache.flink.connector.redis.sink2;

import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.XAddParams;

import java.util.Map;

public class StreamWriteRequest implements RedisWriteRequest {

    private final String key;
    private final Map<String, String> value;

    public StreamWriteRequest(String key, Map<String, String> value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public void write(UnifiedJedis jedis) {
        jedis.xadd(key, XAddParams.xAddParams(), value);
    }

    @Override
    public long getSizeInBytes() {
        long size = 9;
        for (Map.Entry<String, String> entry: value.entrySet()) {
            size += 10 + entry.getKey().length() + entry.getValue().length();
        }
        return size;
    }
}
