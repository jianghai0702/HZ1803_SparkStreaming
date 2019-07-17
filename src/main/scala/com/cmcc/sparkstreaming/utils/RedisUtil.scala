package com.cmcc.sparkstreaming.utils

import java.util

import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

object RedisUtil {
  /**
    * 通过groupId获取offset数据
    *
    * 1.首先会通过groupid去redis数据库查找修改数据
    *
    * @param groupId
    * @return 主题-分区: 偏移量
    */
  def apply(groupId: String):Map[TopicPartition,Long] = {
    //
    var offsetMap = Map[TopicPartition, Long]()
    //  获取redis数据库连接
    val jedis = RedisCP.getConn()
    //  查询redis所有的topicAnd       tpoic01-1 10000
    val topicPartition_Offset: util.Map[String, String] = jedis.hgetAll(groupId)
    //  将map转list, list里面是元组，第一个是topicPartition， 第二个是offset
    val list: List[(String, String)] = topicPartition_Offset.toList
    //
    for (topicPL <- list) {
      val fields = topicPL._1.split("\\-")
      val topicPL_tmp = new TopicPartition(fields(0), fields(1).toInt)
      offsetMap += (topicPL_tmp -> topicPL._2.toLong)
    }
    offsetMap
  }
}
