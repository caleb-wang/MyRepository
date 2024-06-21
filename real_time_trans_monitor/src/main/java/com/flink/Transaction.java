package com.flink;

/**
 * @author wanglb@belink.com
 * @version V1.0
 * @title
 * @description 创建交易数据类
 * @date 2024-05-31 23:16
 */
public class Transaction {

    public String transactionId;
    public String accountId;
    public double amount;
    public long timestamp;

    public Transaction() {}

    public Transaction(String transactionId, String accountId, double amount, long timestamp) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId='" + transactionId + '\'' +
                ", accountId='" + accountId + '\'' +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }

}
