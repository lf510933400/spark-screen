package com.desheng.bigdata.bigscreen.jobs

import java.util.Date

import com.desheng.bigdata.common.{DateUtil, HBaseUtil}
import com.desheng.bigdata.constants.{FieldIndex, HBaseConf}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.hbase.TableName
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 大屏业务统计作业
  *     主要通过scala+sparkStreaming+HBase来完成
  */
object CalcBigscreenApplication {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)

        if(args == null || args.length < 3) {
            println("Parameter Errors! Usage: <batchInterval> <topic> <group>")
            System.exit(-1)
        }
        val Array(batchInterval, topic, group) = args
        val conf = new SparkConf()
                    .setAppName(s"${CalcBigscreenApplication.getClass.getSimpleName}")
                    .setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(batchInterval.toInt))

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> group,
            "auto.offset.reset" -> "earliest"
        )
        val messages:InputDStream[ConsumerRecord[String, String]] = KafkaManager.createMsg(ssc, topic.split(",").toSet, kafkaParams, group, curator)
        messages.foreachRDD((rdd, bTime) => {
            if(!rdd.isEmpty()) {
                println("-------------------------------------------")
                println(s"Time: $bTime")
                process(rdd)
                println("-------------------------------------------")
                //更新offset到zk中
                // java rdd isInstanceOf HashOffsetRanges
                KafkaManager.storeOffset(curator, group, rdd.asInstanceOf[HasOffsetRanges].offsetRanges)
            }
        })
        ssc.start()
        ssc.awaitTermination()
    }
    /**
      * 业务处理的过程
      *     实时总交易额   create 'sale_total_amt', 'cf'
            店铺销售排行   create 'shop_amt_rank', 'cf'
            实时交易额变化趋势   create 'time_amt_trend', 'cf'
        第一步：将原始数据进行转化操作
        第二步：分布统计
        第三步：存储结果
        数据格式：
            time, url, product_id, name, keyword, rank, current_price, original_price, sales_count, shipping_address, shop_id, comments_count, shop_name,
        数据样例：
            2018-02-28 14:51:00^https://item.taobao.com/item.htm?id=542076200484^542076000000^保罗长袖polo衫t恤男翻领男士纯棉秋衣休闲衣服中年男装秋装上衣^男装^第6页第31个^1680^1680^6^广东佛山^73077284^8^杰思卡服饰品牌店
      * @param rdd
      */
    def process(rdd:RDD[ConsumerRecord[String, String]]):Unit = {
        val baseRDD:RDD[String] = rdd.map(record => {
            val msg = record.value()
            val fields = msg.split("\\^")
            if(fields.length != 13) {//异常数据处理
                ""
            } else {
                //时间字段
                val time = fields(FieldIndex.INDEX_TIME)
                val product = fields(FieldIndex.INDEX_PRODUCT_ID)
                val productName = fields(FieldIndex.INDEX_PRODUCT_NAME)
                var priceStr = fields(FieldIndex.INDEX_CURRENT_PRICE)
                //price有可能会有start-end
                if(priceStr.contains("-")) {
                    priceStr = priceStr.split("-")(0).trim
                }
                val price = priceStr.toDouble
                val shopId = fields(FieldIndex.INDEX_SHOP_ID)
                val shopName = fields(FieldIndex.INDEX_SHOP_NAME)

                s"${time}^${product}^${productName}^${price}^${shopId}^${shopName}"
            }
        })
      //将rdd缓存起来提高效率
        baseRDD.cache

       processAllAmt(baseRDD)
       processShopRank(baseRDD)
        processAmtTrend(baseRDD)
        //卸载rdd
        baseRDD.unpersist()
    }
    def processAllAmt(baseRDD:RDD[String]): Unit = {
        println("--------执行总交易额的统计---------")
        val curBatchAmt:Double = baseRDD.map(str => {
            if(str.equals("")) {
                0.0
            } else {
                val fields = str.split("\\^")
                fields(3).toDouble
            }
        }).sum()
        print("当前交易额是："+curBatchAmt)
        /*
            将结果写到hbase对应的表中即可 'sale_total_amt', 'cf'
            逻辑：
                总金额=历史数据+本批次金额
                1)从Hbase中获取历史金额

                2)更新历史数据
            如果使用hbase等操作，需要两步才能完成
            而如果是redis，一步足以(incrByFloat)
         */
       /* val connection = HBaseUtil.getConnection;
        val totalAmtTbl = connection.getTable(TableName.valueOf(HBaseConf.TABLE_SALE_TOTAL_AMT))
        val histAmtResult = totalAmtTbl.get(new Get(HBaseConf.RK_SALE_TOTAL_AMT))

        var curTotalAmt = curBatchAmt
        if(!histAmtResult.isEmpty) {
            val histAmt = new String(histAmtResult.getValue(HBaseConf.CF_SALE_TOTAL_AMT, HBaseConf.COL_SALE_TOTAL_AMT)).toDouble
            //总的
            curTotalAmt += histAmt
        }
        //更新回去
        val put = new Put(HBaseConf.RK_SALE_TOTAL_AMT)
        put.addColumn(HBaseConf.CF_SALE_TOTAL_AMT, HBaseConf.COL_SALE_TOTAL_AMT, curTotalAmt.toString.getBytes())
        totalAmtTbl.put(put)
        totalAmtTbl.close()*/
        val connection = HBaseUtil.getConnection;
        val totalAmtTbl = connection.getTable(TableName.valueOf(HBaseConf.TABLE_SALE_TOTAL_AMT))
        HBaseUtil.addAmt(totalAmtTbl,
            HBaseConf.RK_SALE_TOTAL_AMT,
            HBaseConf.CF_SALE_TOTAL_AMT,
            HBaseConf.COL_SALE_TOTAL_AMT,
            curBatchAmt)
        totalAmtTbl.close()
        HBaseUtil.release(connection)
    }
  /**
    * {time} ${product} ${productName} ${price} ${shopId} ${shopName}
    * 店铺的名称 + 销售额
    */
  def processShopRank(baseRDD:RDD[String]): Unit = {
    println("--------执行店铺排行的统计---------")
    val shop2Price:RDD[(String, Double)] = baseRDD.map(str => {
      if(str.equals("")) {
        ("", 0.0)
      } else {
        val fields = str.split("\\^")
        val shopId = fields(4)
        val shop = fields(5)
        val price = fields(3).toDouble
        (shopId, price)
      }
    })/*.filter(!_._1.equals("")) //这里经过过滤之后下面就不需要进行非空判断了 */
      .reduceByKey(_+_)


    shop2Price.foreachPartition(partition => {
      if(!partition.isEmpty) {
        val connection = HBaseUtil.getConnection;
        val shopTbl = connection.getTable(TableName.valueOf(HBaseConf.TABLE_SHOP_AMT_RANK))
        partition.foreach{case (shopId, price) => {
          if(!shopId.equals("")) {
            HBaseUtil.addAmt(shopTbl, shopId.getBytes, HBaseConf.CF_SHOP_AMT_RANK, HBaseConf.COL_SHOP_AMT_RANK, price)
          }
        }}
        shopTbl.close()
        HBaseUtil.release(connection)
      }
    })
  }
  /**
    * 时间趋势
    * 2018-03-01 10:10:00
    * @param baseRDD
    */
  def processAmtTrend(baseRDD:RDD[String]): Unit = {
    println("--------执行交易趋势的统计---------")
    val time2Amt = baseRDD.map(str => {
      if(str.equals("")) {
        ("", 0.0)
      } else {
        val fields = str.split("\\^")
        //                val time = fields(0).trim
        val time = DateUtil.formatTime(new Date())
        val price = fields(3).toDouble
        (time, price)
      }
    }).filter(!_._1.equals(""))
      .reduceByKey(_+_)

    time2Amt.foreachPartition(partition => {
      if(!partition.isEmpty) {
        val connection = HBaseUtil.getConnection;
        val timeTbl = connection.getTable(TableName.valueOf(HBaseConf.TABLE_TIME_AMT_TREND))

        partition.foreach{case (time, amt) => {
          val timestampe = DateUtil.parseTime(time).toString.reverse

          HBaseUtil.addAmt(timeTbl,
            timestampe.getBytes(),
            HBaseConf.CF_TIME_AMT_TREND,
            HBaseConf.COL_TIME_AMT_TREND,
            amt
          )
        }}
        timeTbl.close()
        HBaseUtil.release(connection)
      }
    })
  }
    val curator = {
      print("提交到github测试-------------")
        val client = CuratorFrameworkFactory.builder()
                        .namespace("mykafka")
                        .connectString("hadoop01:2181,hadoop02:2181,hadoop03:2181")
                        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                        .build()
        client.start()//在使用之前必须先启动
        client
    }
}
