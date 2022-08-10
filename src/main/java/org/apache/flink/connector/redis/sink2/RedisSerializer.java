package org.apache.flink.connector.redis.sink2;

import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

public interface RedisSerializer<T> extends Function, Serializable {

    RedisWriteRequest serialize(T input);
}
