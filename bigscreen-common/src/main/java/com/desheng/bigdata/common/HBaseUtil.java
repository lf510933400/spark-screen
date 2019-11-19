package com.desheng.bigdata.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import java.io.IOException;
import java.util.LinkedList;

/**
 * 操作HBase的工具类，主要作用，提供hbase的Connection用于增加和获取数据
 * 作用相当于连接池
 */
public class HBaseUtil {
    //构建一个池子
    private static LinkedList<Connection> pool = new LinkedList<Connection>();

    private HBaseUtil() {

    }
    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            for (int i = 0; i < 10; i++) {
                Connection connection = ConnectionFactory.createConnection(conf);
                pool.push(connection);
            }
        } catch (IOException e) {
            throw new RuntimeException("初始化异常。。。。");
        }
    }

    public static Connection getConnection() {
        while(pool.isEmpty()) {
            try {
                System.out.println("连接池为空，请稍等一下再来");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return pool.poll();
    }

    public static void release(Connection connection) {
        pool.push(connection);
    }


    public static void addAmt(Table table, byte[] rk, byte[] cf, byte[] col, double curBatchAmt) {
        Result histAmtResult = null;
        try {
            histAmtResult = table.get(new Get(rk));
            double curTotalAmt = curBatchAmt;
            if(!histAmtResult.isEmpty()) {
                double histAmt = Double.valueOf(new String(histAmtResult.getValue(cf, col)));
                //总的
                curTotalAmt += histAmt;
            }
            //更新回去
            Put put = new Put(rk);
            put.addColumn(cf, col, (curTotalAmt + "").getBytes());
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException {
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf("xyz"));
        Result result = table.get(new Get("1".getBytes()));
        System.out.println(new String(result.getValue("cf".getBytes(), "val".getBytes())));
    }
}
