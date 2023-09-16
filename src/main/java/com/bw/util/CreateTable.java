package com.bw.util;
import com.sjsd.util.HBaseConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 创建hbase表
 */
public class CreateTable {
    public static void main(String[] args) {
        try {
            String tableName = "users";
            List<String> columnFamilies = new ArrayList<>();
            columnFamilies.add("uf");
            HBaseUtils.createTable(tableName, columnFamilies);

        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
