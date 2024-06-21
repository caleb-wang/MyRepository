package com.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author wanglb@belink.com
 * @version V1.0
 * @title
 * @description 创建流处理应用
 * @date 2024-05-31 23:20
 */
public class TransactionMonitor {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "transaction-monitor");

        DataStream<String> transactions = env.addSource(new FlinkKafkaConsumer<>("transactions", new SimpleStringSchema(), properties));

        DataStream<Transaction> transactionStream = transactions.map((MapFunction<String, Transaction>) value -> {
            String[] fields = value.split(",");
            return new Transaction(fields[0], fields[1], Double.parseDouble(fields[2]), Long.parseLong(fields[3]));
        });

        transactionStream.keyBy(transaction -> transaction.accountId)
                .process(new FraudDetector())
                .print();

        env.execute("Transaction Monitoring");
    }
}
