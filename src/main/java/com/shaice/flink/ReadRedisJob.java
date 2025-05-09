package com.shaice.flink;

import com.shaice.flink.redis.RedisSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadRedisJob {
    public void startJob() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> sourceStream = env.addSource(new RedisSource());

        // Print the result stream to the console
        sourceStream.print();

        // Execute the Flink job
        env.execute();
    }
}
