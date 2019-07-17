package com.cmcc.sparkstreaming.utils

import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
  * 分析工具
  */
object AnalysticUtil {

  /**
    * 1.1 创建订单
    */
  def makeNewOrder(): Unit = {

  }


  /**
    * 1.2 支付请求
    */
  def sendPayReq(): Unit = {

  }


  /**
    * 1.3 支付通知
    */
  def payNotifyReq(): Unit = {

  }



  /**
    * 1.4 充值请求
    */
  def sendRechargeReq(): Unit = {

  }


  /**
    * 1.5 充值通知
    */
  def reChargeNotifyReq(rdd:RDD[(String, String, List[Double] ,String)] ): Unit = {
    /**
      * 1.1 业务概况
      *
      * 1)	统计全网的充值订单量, 充值金额, 充值成功数
      * 2)	实时充值业务办理趋势, 主要统计全网每小时的订单量数据和成功率
      */
    rdd.map(tup => (tup._1,tup._3)).reduceByKey((list1, list2) => {
        list1.zip(list2).map(p1 => p1._1 + p1._2)
    }).foreachPartition(iter => {
      val jedis: Jedis = RedisCP.getConn()
      iter.foreach(p1 => {
        jedis.hincrBy(p1._1, "totalOrder", p1._2(0).toLong ) //订单总量
        jedis.hincrByFloat(p1._1, "totalMoney", p1._2(1).toFloat)  //订单金额
        jedis.hincrBy(p1._1, "totalSuccessOrder", p1._2(2).toLong ) //订单成功数
        jedis.hincrBy(p1._1, "totalMillisTime", p1._2(3).toLong ) //总时长
        jedis.expire(p1._1,  2 * 24 * 60 * 60 )    //有效期,30天
      })
      jedis.close()
    })



    rdd.map(p1 => (p1._2,p1._3)).reduceByKey((list1, list2) => {
      list1.zip(list2).map(p1 => p1._1 + p1._2)
    }).foreachPartition(iter => {
      val jedis: Jedis = RedisCP.getConn()
      iter.foreach(p1 => {
        jedis.hincrBy(p1._1, "totalOrder_hour2", p1._2(0).toLong ) //每小时订单总量
        jedis.hincrByFloat(p1._1, "totalMoney_hour2", p1._2(1).toFloat)  //每小时订单金额
        jedis.hincrBy(p1._1, "totalSuccessOrder_hour2", p1._2(2).toLong ) //每小时订单成功数
        jedis.hincrBy(p1._1, "totalMillisTime_hour2", p1._2(3).toLong ) //每小时总时长
        jedis.expire(p1._1,  2 * 24 * 60 * 60 )    //有效期,30天
      })
      jedis.close()
    })


    /**
      * 1.2 业务质量：
      *
      * 1. 统计每小时各个省份的充值失败数据量
      */
    //: RDD[((String, String), List[Double])]
    rdd.map(p1 => ((p1._2, p1._4), p1._3)).reduceByKey((list1, list2) => {
      list1.zip(list2).map(p1 => p1._1 + p1._2)
    }).foreachPartition(iter => {
      iter.foreach(p1 => {
        // 插入数据
        ScalikeUtil.insert12(p1)
      })
    })


    /**
      * 1.3 充值订单省份 TOP10
      *
      * //(20170412,2017041203,List(1.0, 5000.0, 1.0, 15203.0),湖北)
      * //(20170412,2017041203,List(1.0, 5000.0, 1.0, 36320.0),江苏)
      * 以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率
      */
      rdd.map(p1 => ((p1._1, p1._4), p1._3)).reduceByKey((list1,list2) => {
        list1.zip(list2).map(p1 => p1._1+p1._2)
      }).foreachPartition(iter => {
        iter.foreach(p1 => {
          // 插入数据
          ScalikeUtil.insert13(p1)
        })
      })



    /**
      * 1.4 实时充值情况分布
      *
      * //(20170412,2017041203,List(1.0, 5000.0, 1.0, 15203.0),湖北)
      * //(20170412,2017041203,List(1.0, 5000.0, 1.0, 36320.0),江苏)
      * 以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率
      */
    rdd.map(p1 => ((p1._2, p1._4), p1._3)).reduceByKey((list1,list2) => {
      list1.zip(list2).map(p1 => p1._1+p1._2)
    }).foreachPartition(iter => {
      iter.foreach(p1 => {
        // 插入数据
        ScalikeUtil.insert14(p1)
      })
    })

  }
}
