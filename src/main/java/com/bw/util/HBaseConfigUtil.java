package com.sjsd.util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/*
    读取核心配置
 */
public class HBaseConfigUtil {
    public static Configuration getHBaseConfiguration() {
        //读取配置文件
        Configuration configuration = HBaseConfiguration.create();
        //设置zookeeper的主机名和端口号
        configuration.set("hbase.zookeeper.quorum", "hadoop5");
        configuration.set("hbase.zookeeper.property.client", "2181");
        return configuration;
    }
}
