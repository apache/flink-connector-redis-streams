package org.apache.flink.connector.redis.sink2;

import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.commands.PipelineCommands;

import java.io.Serializable;

public interface RedisWriteRequest extends Serializable {
    void write(JedisCommands jedis);

    void write(PipelineCommands pipe);

    long getSizeInBytes();
}
