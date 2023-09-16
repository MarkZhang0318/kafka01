package com.bw.kafka02;

import com.bw.util.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public abstract class BaseConsumer {
    private String KAFKA_BROKER_URL;

    //指定Topic，需要子类指定
    protected abstract String getTopicName();

    //指定是否手动提交offset
    protected abstract Boolean getKafkaAutoCommit();

    //获取consumer_group_id
    protected abstract String getConsumerGroupId();

    public void initialize(Properties properties) {
        this.KAFKA_BROKER_URL = properties.getProperty(LoadConfig.BROKER_URL_NAME);
        // this.hbaseSite = properties.getProperty(LoadConfig.HBASE_SITE);
    }

    protected void consume(IngestionTarget target) throws Exception {
        if (KAFKA_BROKER_URL == null && KAFKA_BROKER_URL.length() == 0) {
            throw new Exception("KAFKA_BROKER_URL为空");
        }

        Properties properties = new Properties();
        properties.put("bootstrap.servers", this.KAFKA_BROKER_URL);
        properties.put("group.id", this.getConsumerGroupId());
        properties.put("enable.auto.commit", this.getKafkaAutoCommit() ? "true" : "false");
        properties.put("request.timeout", "180000");
        properties.put("session.timeout", "120000");
        properties.put("max.poll.records", "64");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(this.getTopicName()));

        consumersPoll(consumer,target);

    }

    protected void consumersPoll(KafkaConsumer<String, String> consumer, IngestionTarget target) {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);

                int recordsCount = (records != null) ? records.count() : 0;
                if (recordsCount <= 0) {
                    continue;
                } else {
                    if (target == IngestionTarget.HDFS) {
                        writeInHDFS(records);
                    } else {
                        writeInHBase(records);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }


    }
    private String hbaseSite = null;
    //获取表名
    protected abstract String getHbaseTableName();
    //确认是否是头信息
    protected abstract Boolean isHeader(String[] items);

    //确认是否符合格式
    protected abstract Boolean isValid(String[] items);
    //解析并生成可以放入HBase的List
    protected abstract List<Put> parse(String[] items);

    // 判断写入的位置
    protected static IngestionTarget determineTarget(String target) {
        return (target != null && target.toLowerCase().equals("hdfs")) ? IngestionTarget.HDFS : IngestionTarget.HBASE;
    }

    protected void writeInHBase(ConsumerRecords<String, String> records) throws Exception {
        /* if (this.hbaseSite == null || this.hbaseSite.isEmpty()) {
            throw new Exception("hbase-site.xml is not initialized");
        }

        Configuration config = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(config); */

        try {
            // Table table = conn.getTable(TableName.valueOf(this.getHbaseTableName()));
            HTable table = HBaseUtils.initHbaseClient(this.getHbaseTableName());
            try {
                long count = 0;
                //如果第一条数据是头数据，需要跳过
                long passHead = 0;
                for (ConsumerRecord<String, String> record : records) {
                    String[] items = record.value().split(",");
                    if (passHead == 0 && isHeader(items)) {
                        passHead++;
                        continue;
                    }

                    //解析数据放入HBase
                    List<Put> puts = isValid(items) ? parse(items) : null;
                    if (puts != null && puts.size() > 0) {
                        table.put(puts);
                        count += puts.size();
                    }
                    System.out.println("插入的数据数" + count);
                }

            } finally {
                table.close();
            }
        } finally {
            // conn.close();
        }
    }


    protected abstract void writeInHDFS(ConsumerRecords<String, String> records);

    /* 程序入口
     *  */
    protected static <T extends BaseConsumer> void start(Class<T> clsConsumer, String[] args) throws Exception {
        if (args.length < 1) {
            throw new Exception("启动参数错误");
        } else {
            BaseConsumer consumer = clsConsumer.newInstance();
            Properties properties = new Properties();
            properties.setProperty(LoadConfig.BROKER_URL_NAME, LoadConfig.BROKER_URL_VALUE);
            consumer.initialize(properties);
            consumer.consume(determineTarget(args[0]));

        }
    }




}
