package com.shaice.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class ReadIcebergTableJob {
    public void startJob() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "CREATE TABLE iceberg_table (" +
                        "status BOOLEAN, " +
                        "msg STRING, " +
                        "createTime BIGINT" +
                        ") WITH (" +
                        "'connector' = 'iceberg'," +
                        "'catalog-type' = 'hadoop'," +
                        "'catalog-name' = 'local_catalog'," +
                        "'warehouse' = 'file:///Users/zhangyining/flink/data'," +
                        "'format-version' = '1'" +
                        ")"
        );

        Table resultTable = tableEnv.sqlQuery("select * from iceberg_table");
        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);
        resultStream.print();
        env.execute();
    }
}
