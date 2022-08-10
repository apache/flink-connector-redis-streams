package org.apache.flink.connector.redis.sink2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class RedisAsyncWriter<IN> extends AsyncSinkWriter<IN, RedisWriteRequest> {

    private final UnifiedJedis jedis;

    public RedisAsyncWriter(
            UnifiedJedis jedis,
            RedisConverter<IN> converter,
            RedisSinkConfig sinkConfig,
            Sink.InitContext context) {
        super(
                converter,
                context,
                sinkConfig.maxBatchSize,
                sinkConfig.maxInFlightRequests,
                sinkConfig.maxBufferedRequests,
                sinkConfig.maxBatchSizeInBytes,
                sinkConfig.maxTimeInBufferMS,
                sinkConfig.maxRecordSizeInBytes);
        this.jedis = jedis;
    }

    @Override
    protected void submitRequestEntries(
            List<RedisWriteRequest> requests, Consumer<List<RedisWriteRequest>> consumer) {
        List<RedisWriteRequest> toRetry = new ArrayList<>();
        for (RedisWriteRequest request : requests) {
            try {
                request.write(jedis);
            } catch (JedisDataException de) {
                // not worth retrying; will fail again
                // TODO
            } catch (Exception e) {
                toRetry.add(request);
            }
        }
        consumer.accept(toRetry);
    }

    @Override
    protected long getSizeInBytes(RedisWriteRequest request) {
        return request.getSizeInBytes();
    }
}
