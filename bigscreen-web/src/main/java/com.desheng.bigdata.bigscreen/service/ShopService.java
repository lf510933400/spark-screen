package com.desheng.bigdata.bigscreen.service;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.desheng.bigdata.common.DateUtil;
import com.desheng.bigdata.common.HBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import com.desheng.bigdata.constants.HBaseConf;

import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.Random;
import java.util.TreeSet;

/**
 * 核心业务实现类
 */
public class ShopService {

    /**
     * 店铺排行
     * {[{shop_name:xx,shop_amt_sum:xxx},{}]}
     * @return
     */
    public static Object getShopRanking() {
        TreeSet<JSONObject> ts = new TreeSet<JSONObject>(new Comparator<JSONObject>() {
            @Override
            public int compare(JSONObject o1, JSONObject o2) {
                Double sam1 = o1.getDouble("shop_amt_sum");
                Double sam2 = o2.getDouble("shop_amt_sum");
                return sam2.compareTo(sam1);
            }
        });
        Connection connection = HBaseUtil.getConnection();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(HBaseConf.TABLE_SHOP_AMT_RANK()));
            ResultScanner results = table.getScanner(new Scan());
            for(Result result : results) {
                double shopAmt = Double.valueOf(new String(result.getValue(HBaseConf.CF_SHOP_AMT_RANK(), HBaseConf.COL_SHOP_AMT_RANK())));
                String shopId = new String(result.getRow());
                JSONObject json = new JSONObject();
                json.put("shop_name", shopId);
                json.put("shop_amt_sum", shopAmt);
                ts.add(json);
                if(ts.size() > 5) {
                    ts.pollLast();
                }
            }
            //最多就只能留下5个
            JSONArray jsonArray = new JSONArray();
            jsonArray.addAll(ts);
            return jsonArray.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                HBaseUtil.release(connection);
            }
        }
        return null;
    }

    /**
     * 汇总量
     *
     * @return
     */
    public static Object getAllAmt() {
        Connection connection = HBaseUtil.getConnection();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(HBaseConf.TABLE_SALE_TOTAL_AMT()));
            Result result = table.get(new Get(HBaseConf.RK_SALE_TOTAL_AMT()));
            return new String(result.getValue(HBaseConf.CF_SALE_TOTAL_AMT(), HBaseConf.COL_SALE_TOTAL_AMT()));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                HBaseUtil.release(connection);
            }
        }
        return "0";
    }

    /**
     * 曲线轴汇总量
     *
     * @return
     */
    static Random random = new Random();
    public static Object getXTimeAmt() {
        Connection connection = HBaseUtil.getConnection();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(HBaseConf.TABLE_TIME_AMT_TREND()));

            long curTM = System.currentTimeMillis() - 5 * 1000;
            byte[] rk = new StringBuilder(curTM + "").reverse().toString().getBytes();
            Result result = table.get(new Get(rk));
            double timeAmt = random.nextDouble() * 1000;
            if(!result.isEmpty()) {
                byte[] amtBytes = result.getValue(HBaseConf.CF_TIME_AMT_TREND(), HBaseConf.COL_TIME_AMT_TREND());
                timeAmt = Double.valueOf(new String(amtBytes));
            }
            JSONObject jsonObj = new JSONObject();
            jsonObj.put("x_time", DateUtil.formatTime(new Date(curTM)));
            jsonObj.put("time_amtSum", timeAmt);
            return jsonObj.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                HBaseUtil.release(connection);
            }
        }
        return null;
    }

    public static void main(String[] args) {
    }
}
