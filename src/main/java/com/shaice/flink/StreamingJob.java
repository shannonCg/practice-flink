package com.shaice.flink;

public class StreamingJob {
    public static void main(String[] args) throws Exception{
//        ConsumeAndProduceKafkaCommitByFlinkJob job = new ConsumeAndProduceKafkaCommitByFlinkJob();
//        job.startJob();

//        ConsumeAndProduceKafkaAutoCommitJob job = new ConsumeAndProduceKafkaAutoCommitJob();
//        job.startJob();

//        ConsumeKafkaAndSaveIcebergTableWithParquetFormatJob job = new ConsumeKafkaAndSaveIcebergTableWithParquetFormatJob();
//        job.startJob();

//        ReadIcebergTableJob job = new ReadIcebergTableJob();
//        job.startJob();

//        ConsumeKafkaAndSinkRedisJob job = new ConsumeKafkaAndSinkRedisJob();
//        job.startJob();

        ReadRedisJob job = new ReadRedisJob();
        job.startJob();
    }
}
