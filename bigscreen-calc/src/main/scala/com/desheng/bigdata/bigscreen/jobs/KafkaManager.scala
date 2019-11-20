package com.desheng.bigdata.bigscreen.jobs

import org.apache.curator.framework.CuratorFramework
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.{JavaConversions, mutable}

object KafkaManager {

    def createMsg(ssc:StreamingContext, topics:Set[String],
                  kafkaParams:Map[String, Object],
                  group:String,
                  curator:CuratorFramework): InputDStream[ConsumerRecord[String, String]] = {
        /*
            sparkstreaming+kafka
            locationStrategy(本地化策略):
                从kafka0.10的版本开始，消费者每次都是预先拉取对应分区中的数据，所以把该consumer的信息cache到相近
                的executor上面可以提高执行的效率，而不是每次调度作业都重新的为不同的partition创建新的consumer。
                PreferBrokers：executor(计算)和kafka-broker(数据)在同一台节点上的时候使用。
                PreferConsistent（most）：大多情况下使用这种情况，当kafka-partition分散在executor上面的时候，二者不是
                    一一对应的时候。
                PreferFixed：如果partition存储在特定的主机之上，而且kafka集群环境配置参差不齐，这个时候就可以将partition和
                    对应的host对应起来进行消费。
            consumerStrategy(消费策略):
         */
        val offsets:Map[TopicPartition, Long] = getOffsets(curator, topics, group)
        var messages:InputDStream[ConsumerRecord[String, String]] = null
        if(offsets.isEmpty) {
            messages = KafkaUtils.createDirectStream[String, String](ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
            )
        } else {
            messages = KafkaUtils.createDirectStream[String, String](ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets)
            )
        }
        messages
    }

    /**
      * 获取指定topic的partition的偏移量
      * 怎么操作zk--->zookeeper的框架CuratorFramework
      *
      * @param topics
      * @param group
      * @return
      */
    def getOffsets(curator:CuratorFramework, topics: Set[String],
                   group: String): Map[TopicPartition, Long] = {
        val map = mutable.Map[TopicPartition, Long]()
        for(topic <- topics) {
            //mykafka/offsets/${topic}/${group}/${partition}
            val parent = s"offsets/${topic}/${group}"
            //判断parent是否存在
            checkExist(curator, parent)
            //读取对应parent下面的所有的partition信息
            for(partition <- JavaConversions.asScalaBuffer(curator.getChildren.forPath(parent))) {
                val fullPath = s"${parent}/${partition}"
                val offset = new String(curator.getData.forPath(fullPath)).toLong
                map.put(new TopicPartition(topic, partition.toInt), offset)
            }
            /*//因为有嵌套for，所以无法做返回，只能在外层的yield中做返回
            val offsets = for(partition <- JavaConversions.asScalaBuffer(curator.getChildren.forPath(parent))) yield {
                val fullPath = s"${parent}/${partition}"
                val offset = new String(curator.getData.forPath(fullPath)).toLong
                (new TopicPartition(topic, partition.toInt), offset)
            }*/
        }

        map.toMap
    }

    def checkExist(curator: CuratorFramework, path: String) = {
        if(curator.checkExists().forPath(path) == null) {
            //当前path必然存在
            curator.create().creatingParentsIfNeeded().forPath(path)
        }
    }
    /**
      * mykafka/offsets/${topic}/${group}/${partition}
      */
    def storeOffset(curator: CuratorFramework, group:String, offsetRanges: Array[OffsetRange]): Unit = {
        for(offsetRange <- offsetRanges) {
            val partition = offsetRange.partition
            val topic = offsetRange.topic
            val offset = offsetRange.untilOffset
            val fullPath = s"offsets/${topic}/${group}/${partition}"
            checkExist(curator, fullPath)
            //更新数据
            curator.setData().forPath(fullPath, ("" + offset).getBytes())
        }
    }
}
