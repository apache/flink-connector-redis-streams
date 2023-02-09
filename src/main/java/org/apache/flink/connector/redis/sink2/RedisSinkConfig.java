package org.apache.flink.connector.redis.sink2;

import java.io.Serializable;

public class RedisSinkConfig implements Serializable {

    public final int maxBatchSize;
    public final int maxInFlightRequests;
    public final int maxBufferedRequests;
    public final long maxBatchSizeInBytes;
    public final long maxTimeInBufferMS;
    public final long maxRecordSizeInBytes;

    public RedisSinkConfig(
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes) {
        this.maxBatchSize = maxBatchSize;
        this.maxInFlightRequests = maxInFlightRequests;
        this.maxBufferedRequests = maxBufferedRequests;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.maxTimeInBufferMS = maxTimeInBufferMS;
        this.maxRecordSizeInBytes = maxRecordSizeInBytes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int maxBatchSize = 10;
        private int maxInFlightRequests = 1;
        private int maxBufferedRequests = 100;
        private long maxBatchSizeInBytes = 110;
        private long maxTimeInBufferMS = 1_000;
        private long maxRecordSizeInBytes = maxBatchSizeInBytes;

        public Builder maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder maxInFlightRequests(int maxInFlightRequests) {
            this.maxInFlightRequests = maxInFlightRequests;
            return this;
        }

        public Builder maxBufferedRequests(int maxBufferedRequests) {
            this.maxBufferedRequests = maxBufferedRequests;
            return this;
        }

        public Builder maxBatchSizeInBytes(long maxBatchSizeInBytes) {
            this.maxBatchSizeInBytes = maxBatchSizeInBytes;
            return this;
        }

        public Builder maxTimeInBufferMS(long maxTimeInBufferMS) {
            this.maxTimeInBufferMS = maxTimeInBufferMS;
            return this;
        }

        public Builder maxRecordSizeInBytes(long maxRecordSizeInBytes) {
            this.maxRecordSizeInBytes = maxRecordSizeInBytes;
            return this;
        }

        public RedisSinkConfig build() {
            return new RedisSinkConfig(
                    maxBatchSize,
                    maxInFlightRequests,
                    maxBufferedRequests,
                    maxBatchSizeInBytes,
                    maxTimeInBufferMS,
                    maxRecordSizeInBytes);
        }
    }
}
