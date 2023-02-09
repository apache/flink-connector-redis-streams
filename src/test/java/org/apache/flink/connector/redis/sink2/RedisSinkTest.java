package org.apache.flink.connector.redis.sink2;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.redis.RedisITCaseBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisSinkTest extends RedisITCaseBase {

    @Test
    public void testStringCommand() throws Exception {
        RedisSerializer<String> serializer = input -> new StringWriteRequest("key-"+input, "value-"+input);
        RedisSinkConfig redisSinkConfig = RedisSinkConfig.builder().build();
        RedisSink<String> underTest = new RedisSink<>(getConfigProperties(), redisSinkConfig, serializer);

        List<String> source = Arrays.asList("one", "two", "three");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.fromCollection(source).sinkTo(underTest);
        env.execute();

        // verify results
        source.forEach(entry -> assertEquals("value-" + entry, jedis.get("key-"+entry)));
    }

    @Test
    public void testStreamCommand() throws Exception {
        RedisSerializer<Tuple3<String, String, String>> serializer =
                input -> {
                    Map<String, String> value = new HashMap<>();
                    value.put(input.f1, input.f2);
                    return new StreamWriteRequest(input.f0, value);
                };
        RedisSinkConfig redisSinkConfig = RedisSinkConfig.builder().build();
        RedisSink<Tuple3<String, String, String>> underTest =
                new RedisSink<>(getConfigProperties(), redisSinkConfig, serializer);

        List<Tuple3<String, String, String>> source =
                Arrays.asList(
                        Tuple3.of("one", "onekey", "onevalue"),
                        Tuple3.of("two", "firstkey", "firstvalue"),
                        Tuple3.of("two", "secontkey", "secondvalue"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.fromCollection(source).sinkTo(underTest);
        env.execute();

        // verify results
        assertEquals(1, jedis.xlen("one"));
        assertEquals(2, jedis.xlen("two"));
    }
}
