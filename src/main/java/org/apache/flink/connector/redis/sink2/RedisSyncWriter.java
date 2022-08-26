package org.apache.flink.connector.redis.sink2;

import org.apache.flink.api.connector.sink2.SinkWriter;
import redis.clients.jedis.Connection;
import redis.clients.jedis.MultiNodePipelineBase;
import redis.clients.jedis.Pipeline;

import java.io.IOException;

public class RedisSyncWriter<IN> implements SinkWriter<IN> {

    private final Connection conn;
    private final Pipeline pipe;
    private final MultiNodePipelineBase mpipe;

    private final RedisSerializer<IN> serializer;

    public RedisSyncWriter(
            Connection connection, RedisSerializer<IN> serializer) {
        this.conn = connection;
        this.pipe = new Pipeline(this.conn);
        this.mpipe = null;
        this.serializer = serializer;
    }

    public RedisSyncWriter(
            Pipeline pipe, RedisSerializer<IN> serializer) {
        this.conn = null;
        this.pipe = pipe;
        this.mpipe = null;
        this.serializer = serializer;
    }

    public RedisSyncWriter(
            MultiNodePipelineBase mpipe, RedisSerializer<IN> serializer) {
        this.conn = null;
        this.pipe = null;
        this.mpipe = mpipe;
        this.serializer = serializer;
    }

    @Override
    public void write(IN input, Context context) throws IOException, InterruptedException {
        RedisWriteRequest request = serializer.serialize(input);
        if (pipe != null) {
            request.write(pipe);
        } else {
            request.write(mpipe);
        }
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {
        if (pipe != null) {
            pipe.sync();
        } else {
            mpipe.sync();
        }
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        } else {
            mpipe.close();
        }
    }
}
