package org.apache.flink.connector.redis.sink2;

import org.apache.flink.api.connector.sink2.SinkWriter;
import redis.clients.jedis.UnifiedJedis;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

public class RedisSyncWriter<IN> implements SinkWriter<IN> {

    private final UnifiedJedis jedis;

    private final RedisSerializer<IN> serializer;

    private final boolean writeImmediate;

    private final Queue<RedisWriteRequest> queue = new ArrayDeque<>();

    public RedisSyncWriter(
            UnifiedJedis jedis, RedisSerializer<IN> serializer, boolean writeImmediate) {
        this.jedis = jedis;
        this.serializer = serializer;
        this.writeImmediate = writeImmediate;
    }

    @Override
    public void write(IN input, Context context) throws IOException, InterruptedException {
        RedisWriteRequest request = serializer.serialize(input);
        if (writeImmediate) {
            request.write(jedis);
        } else {
            queue.add(request);
        }
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {
        while (!queue.isEmpty()) {
            RedisWriteRequest request = queue.remove();
            request.write(jedis);
        }
    }

    @Override
    public void close() throws Exception {
        jedis.close();
    }
}
