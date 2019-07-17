package com.cmcc.sparksql

import com.alibaba.fastjson.{JSON, JSONObject}
import com.cmcc.sparksql.utils.ScalikeUtil
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import org.apache.spark.rdd.RDD



object CMCC2 {

  // import spark.implicits._
  // import org.apache.spark.sql.functions._
  // spark.sparkContext.setCheckpointDir("C:\\checkpoint")
  val kPATH = "C:\\Users\\jiang\\Desktop\\Spark项目\\hz1803_spark\\src\\main\\resources\\cmcc2.json"
  val spark = SparkSession.builder().appName("CMCC2").master("local[2]").getOrCreate()
  val dateformat = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")


  // code : xxx省
  val map = new mutable.HashMap[String,String]()
  val kCITYPATH = "C:\\Users\\jiang\\Desktop\\Spark项目\\hz1803_spark\\src\\main\\resources\\city.txt"
  val kCityFile: RDD[String] = spark.sparkContext.textFile(kCITYPATH, 2)
  kCityFile.collect().map(line => {
    val fields = line.split("\\s")
    map.put(fields(0), fields(1))
  })
  //广播变量
  val cityMap = spark.sparkContext.broadcast(map)


  def main(args: Array[String]): Unit = {


    val file: RDD[String] = spark.sparkContext.textFile(kPATH,2)

    /**
      * 过滤各种日志
      *
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
      * 获取需要的字段
      *
      * {"bussinessRst":"0000","channelCode":"6900",
      * "chargefee":"1000","clientIp":"117.136.79.101",
      * "endReqTime":"20170412030030230","idType":"01",
      * "interFacRst":"0000","logOutTime":"20170412030030230",
      * "orderId":"384663607178845909","prodCnt":"1",
      * "provinceCode":"200","requestId":"20170412030007090581518228485394",
      * "retMsg":"成功","serverIp":"172.16.59.241",
      * "serverPort":"8088","serviceName":"sendRechargeReq",
      * "shouldfee":"1000","startReqTime":"20170412030030080","sysId":"01"}
      */
    val maped: RDD[(String, String,String, List[Double], String)] = sendRechargeReq.map(dict => {

      //充值结果
      val bussinessRst: String = dict.getString("bussinessRst")
      //是否成功
      val isSuccess: Int = if("0000".equalsIgnoreCase(bussinessRst)) 1 else 0
      //充值金额
      val money: Double = if (isSuccess == 1) dict.getString("chargefee").toDouble else 0.0
      //开始时间 20170412 030017 876364973282669502
      val startTime: String = dict.getString("requestId")
      //结束时间 20170412 030038 958
      val endTime: String = dict.getString("endReqTime")

      //开始时间 20170412
      val startTimeStr8 = startTime.substring(0,8)
      //开始时间 2017041203
      val startTimeStr10 = startTime.substring(0,10)
      //开始时间 2017041203
      val startTimeStr12 = startTime.substring(0,12)
      //开始时间 20170412 0300
      val startTimeStrLong = startTime.substring(0,17)
      //开始时间 20170412 00000 333
      val endTimeStrLong = endTime.substring(0,17)

      //省份
      val provinceCode: String = dict.getString("provinceCode")
      val province: String =  cityMap.value.getOrElse(provinceCode, "")

      //花费总时长
      val startMillisTime: Long = dateformat.parse(startTimeStrLong).getTime
      val endMillisTime: Long = dateformat.parse(endTimeStrLong).getTime
      val totalMillisTime: Long = if (isSuccess == 1) endMillisTime - startMillisTime else 0
      (startTimeStr8,startTimeStr10,startTimeStr12,List[Double](1,money,isSuccess,totalMillisTime), province)
    })

    //(20170412,2017041203,201704120300,List(1.0, 5000.0, 1.0, 15203.0),湖北)
    //(20170412,2017041203,201704120300,List(1.0, 5000.0, 1.0, 36320.0),江苏)
    maped.collect().foreach(println)



    /**
      * 业务失败省份 TOP3（离线处理[每天]）
      *
      * 2.1  业务失败省份 TOP3（离线处理[每天]）
      *
      * //(20170412,2017041203,201704120300,List(1.0, 5000.0, 1.0, 15203.0),湖北)
      * //(20170412,2017041203,201704120300,List(1.0, 5000.0, 1.0, 36320.0),江苏)
      */
    //: RDD[((String, String), List[Double])]
    maped.map(p1 => ((p1._1, p1._5), p1._4)).reduceByKey((list1, list2) => {
      list1.zip(list2).map(p1 => p1._1 + p1._2)
    }).foreachPartition(iter => {
      iter.foreach(p1 => {
        // 插入数据
        ScalikeUtil.insert21(p1)
      })
    })



    /**
      * 充值机构分布
      *
      * 2.2
      * 1)	以省份为维度,统计每分钟各省的充值笔数和充值金额
      * 2)	以省份为维度,统计每小时各省的充值笔数和充值金额
      *
      * //(20170412,2017041203,201704120300,List(1.0, 5000.0, 1.0, 15203.0),湖北)
      * //(20170412,2017041203,201704120300,List(1.0, 5000.0, 1.0, 36320.0),江苏)
      */
    //: RDD[((String, String), List[Double])]
    maped.map(p1 => ((p1._1,p1._2,p1._3, p1._5), p1._4)).reduceByKey((list1, list2) => {
      list1.zip(list2).map(p1 => p1._1 + p1._2)
    }).foreachPartition(iter => {
      iter.foreach(p1 => {
        // 插入数据
        ScalikeUtil.insert22(p1)
      })
    })

    spark.stop()
  }
}
