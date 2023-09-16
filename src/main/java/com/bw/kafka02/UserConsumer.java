package com.bw.kafka02;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class UserConsumer extends BaseConsumer implements ExecutorProgram {
    public static void main(String[] args) throws Exception{
        new UserConsumer().executor(args);

    }

    @Override
    protected String getTopicName() {
        return "demo2";
    }

    @Override
    protected Boolean getKafkaAutoCommit() {
        return false;
    }

    @Override
    protected String getConsumerGroupId() {
        return "consumer_user1";
    }

    @Override
    protected String getHbaseTableName() {
        return "users";
    }

    @Override
    protected Boolean isHeader(String[] items) {
        return isValid(items) && items[0].equals("user_id") && items[1].equals("user_friends");
    }

    /*
    * 简单判断一下数据长度是否符合要求 */
    @Override
    protected Boolean isValid(String[] items) {
        return (items.length > 1);
    }

    @Override
    protected List<Put> parse(String[] items) {
        List<Put> puts = new ArrayList<>();
        String user_id = items[0];
        String[] friends = items[1].split(" ");
        /* Put put = new Put(Bytes.toBytes((user_id + "-" + friends).hashCode()));
        put.addColumn(Bytes.toBytes("uf"), Bytes.toBytes("user_id"), Bytes.toBytes(user_id));
        long count = 0;
        for (String friend : friends) {
            count++;
            put.addColumn(Bytes.toBytes("uf"), Bytes.toBytes("friend" + count), Bytes.toBytes(friend));
        }
        puts.add(put); */
        for (String friend : friends) {
            Put put = new Put(Bytes.toBytes((user_id + "-" + friend).hashCode()));
            put.addColumn(Bytes.toBytes("uf"), Bytes.toBytes("user_id"), Bytes.toBytes(user_id));
            put.addColumn(Bytes.toBytes("uf"), Bytes.toBytes("friend"), Bytes.toBytes(friend));
            puts.add(put);
        }


        return puts;
    }

    @Override
    protected void writeInHDFS(ConsumerRecords<String, String> records) {

    }

    @Override
    public void executor(String[] args) throws Exception {
        try {
            BaseConsumer.start(UserConsumer.class, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void initialize(Properties properties) {
        super.initialize(properties);
    }
}
