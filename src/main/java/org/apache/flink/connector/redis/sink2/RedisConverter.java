package org.apache.flink.connector.redis.sink2;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

public class RedisConverter<T> implements ElementConverter<T, RedisWriteRequest> {

    private final RedisSerializer<T> serializer;

    public RedisConverter(RedisSerializer<T> serializer) {
        this.serializer = serializer;
    }

    @Override
    public RedisWriteRequest apply(T input, SinkWriter.Context context) {
        return serializer.serialize(input);
    }
}
