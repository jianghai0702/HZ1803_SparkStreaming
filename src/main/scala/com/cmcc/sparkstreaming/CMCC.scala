package com.cmcc.sparkstreaming

import com.alibaba.fastjson.{JSON, JSONObject}
import com.cmcc.sparkstreaming.utils.{AnalysticUtil, RedisCP, RedisUtil}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object CMCC {

  val dateformat = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")


  /**
    * SparkStreaming
    */
  val conf: SparkConf = new SparkConf()
    .setAppName("CMCC")
    .setMaster("local[*]")
    //设置每个批次在每个分区拉取kafka的速度
    .set("spark.streaming.kafka.maxRatePerPartition", "100")
    //设置序列化机制
    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
  val sc: StreamingContext = new StreamingContext(conf, Seconds(5))


  // code : xxx省
  val map = new mutable.HashMap[String,String]()
  val spark = SparkSession.builder().appName("CityUtil").master("local[2]").getOrCreate()
  val file: RDD[String] = spark.sparkContext.textFile("C:\\Users\\jiang\\Desktop\\Spark项目\\hz1803_spark\\src\\main\\resources\\city.txt")
  file.collect().map(line => {
    val fields = line.split("\\s")
    //println( fields(0) + " : " + fields(1) )
    map.put(fields(0), fields(1))
  })
  //广播变量
  val cityMap = sc.sparkContext.broadcast(map)




  def main(args: Array[String]): Unit = {




    /**
      * kafka
      *
      * /consumers/group01/offsets/topic01/0/10000
      * /consumers/group01/offsets/topic01/1/19999
      * /consumers/group01/offsets/topic01/2/29999
      */
    val group = "group_cmcc_05"
    val topic = "topic_cmcc_01"
    val brokers = "node22:9092,node23:9092,node24:9092"
    val kafkas = Map[String, Object](
      "bootstrap.servers" -> brokers,
      //key、value的解码方式
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      //从头消费
      "auto.offset.reset" -> "earliest",
      //不需要自动提交offset（kafka0.10可以自己记录offset，但是我们要使用redis手动处理）
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )
    //创建topic集合，可能会消费多个topic
    val topics = Set(topic)



    /**
      * 第一步：从redis获取offset
      */
    val offsetMap: Map[TopicPartition, Long] = RedisUtil(group)



    /**
      * 第二步：通过offset获取kafka数据
      */
    val stream: InputDStream[ConsumerRecord[String, String]] = {
      if (offsetMap.size == 0) {
        KafkaUtils.createDirectStream(
          sc,
          //  本地策略
          //  将数据均匀分配到各个Executor中
          LocationStrategies.PreferConsistent,
          //  消费者策略
          //  可以动态添加分区
          ConsumerStrategies.Subscribe(topics,kafkas)
        )
      } else {
        //不是第一次消费
        KafkaUtils.createDirectStream(
          sc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String,String](offsetMap.keys,kafkas,offsetMap)
        )
      }
    }



    stream.foreachRDD(kafkaRDD => {
      val offsetRanges: Array[OffsetRange] = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

      val file: RDD[String] = kafkaRDD.map(record => record.value())
      //file.collect().map(println)

      /**
        * 1.1 创建订单
        * 1.2 支付请求
        * 1.3 支付通知
        * 1.4 充值请求
        * 1.5 充值通知
        */
      var jsoned: RDD[JSONObject] = file.map(line => JSON.parseObject(line) )
      val makeNewOrder = jsoned.filter( dict => "makeNewOrder".equalsIgnoreCase( dict.getString("serviceName") ) )
      val sendPayReq = jsoned.filter( dict => "sendPayReq".equalsIgnoreCase( dict.getString("serviceName") ) )
      val payNotifyReq = jsoned.filter( dict => "payNotifyReq".equalsIgnoreCase( dict.getString("serviceName") ) )
      val sendRechargeReq = jsoned.filter( dict => "sendRechargeReq".equalsIgnoreCase( dict.getString("serviceName") ) )
      val reChargeNotifyReq = jsoned.filter( dict => "reChargeNotifyReq".equalsIgnoreCase( dict.getString("serviceName") ) )


      /**
        * 充值通知；
        *
        * 需求：
        * 1.充值订单量
        * 2.充值金额
        * 3.充值成功率
        * 4.充值平均时长
        */

      val maped: RDD[(String, String, List[Double] ,String)] = reChargeNotifyReq.map(dict => {

        //充值结果
        val bussinessRst: String = dict.getString("bussinessRst")
        //是否成功
        val isSuccess: Int = if("0000".equalsIgnoreCase(bussinessRst)) 1 else 0
        //充值金额
        val money: Double = if (isSuccess == 1) dict.getString("chargefee").toDouble else 0.0
        //开始时间 20170412 030017 876364973282669502
        val startTime: String = dict.getString("requestId")
        //结束时间 20170412 030038 958
        val endTime: String = dict.getString("receiveNotifyTime")

        //开始时间 20170412
        val startTimeStr8 = startTime.substring(0,8)
        //结束时间 2017041203
        val startTimeStr10 = startTime.substring(0,10)
        //开始时间 20170412 0300
        val startTimeStrLong = startTime.substring(0,17)
        //开始时间 20170412 00000 333
        val endTimeStrLong = endTime.substring(0,17)

        //省份
        val provinceCode: String = dict.getString("provinceCode")
        val province: String =  cityMap.value.getOrElse(provinceCode, "")

        //List[Double](1,"xxx省")

        //花费总时长
        val startMillisTime: Long = dateformat.parse(startTimeStrLong).getTime
        val endMillisTime: Long = dateformat.parse(endTimeStrLong).getTime
        val totalMillisTime: Long = if (isSuccess == 1) endMillisTime - startMillisTime else 0
        (startTimeStr8,startTimeStr10,List[Double](1,money,isSuccess,totalMillisTime), province)
      })


      //(20170412,2017041203,湖北,List(1.0, 5000.0, 1.0, 15203.0))
      //(20170412,2017041203,江苏,List(1.0, 5000.0, 1.0, 36320.0))
      //maped.collect().foreach(println)

      //1
      AnalysticUtil.reChargeNotifyReq(maped)













      /**
        * 第三步：更新offset到redis
        *
        */
      val jedis = RedisCP.getConn()
      for (or <- offsetRanges) {
        //  更新偏移量     tpoic01-1 10000
        //jedis.hset(group, or.topic+"-"+or.partition ,or.untilOffset.toString)
      }
      jedis.close()
    })


    sc.start()
    sc.awaitTermination()

  }
}


//case class User(user_id: Int, name: String, user_gender: Int)