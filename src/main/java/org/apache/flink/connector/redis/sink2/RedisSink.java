package org.apache.flink.connector.redis.sink2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.redis.JedisUtils;

import java.io.IOException;
import java.util.Properties;

public class RedisSink<T> implements Sink<T> {

    private final Properties config;
    private final RedisSerializer<T> serializer;

    private final RedisSinkConfig sinkConfig;

    public RedisSink(Properties config, RedisSinkConfig sinkConfig, RedisSerializer<T> serializer) {
        this.config = config;
        this.sinkConfig = sinkConfig;
        this.serializer = serializer;
    }

    @Override
    public SinkWriter<T> createWriter(Sink.InitContext initContext) throws IOException {
//        return new RedisAsyncWriter<>(
//                JedisUtils.createJedis(config),
//                new RedisConverter<>(serializer),
//                sinkConfig,
//                initContext);
        return new RedisSyncWriter<T>(JedisUtils.createConnection(config), serializer);
    }
}
