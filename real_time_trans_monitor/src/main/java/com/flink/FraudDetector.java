package com.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @author wanglb@belink.com
 * @version V1.0
 * @title
 * @description 创建欺诈检测功能
 * @date 2024-05-31 23:18
 */
public class FraudDetector extends KeyedProcessFunction<String, Transaction, String> {
    private ValueState<Double> lastTransactionAmount;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("lastTransactionAmount", Double.class);
        lastTransactionAmount = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Transaction transaction, Context context, Collector<String> collector) throws Exception {
        Double lastAmount = lastTransactionAmount.value();

        if (lastAmount != null && Math.abs(transaction.amount - lastAmount) > 1000) {
            collector.collect("Potential fraud detected for account " + transaction.accountId +
                    ": Transaction " + transaction.transactionId + " amount " + transaction.amount);
        }

        lastTransactionAmount.update(transaction.amount);
    }
}
