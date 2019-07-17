package com.cmcc.sparksql.utils

import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

object ScalikeUtil {
  /**
    * 加载配置
    */
  DBs.setup() //加载默认配置 db.default.*
  //DBs.setupAll() //加载所有配置
  DBs.setup('project2) //加载自定义配置 db.project2.*


  /**
    * 插入一条数据
    * @param rdd  ((String,String),List[Double])
    *              ((时间,省份),List[Double])
    */
  def insert12(rdd: ((String,String),List[Double]) ): Unit ={
    val failureAmount: Int = rdd._2(0).toInt - rdd._2(2).toInt

    DB.autoCommit(implicit session => {
      SQL("insert into table12(provience, hour, failureAmount) values(?,?,?)")
        .bind(rdd._1._2, rdd._1._1, failureAmount)
        .update()
        .apply()
    })
  }

  /**
    * 插入一条数据
    *
    * @param rdd ((String,String),List[Double])
    *            ((时间,省份),List[Double])
    *            //(20170412,2017041203,List(1.0, 5000.0, 1.0, 15203.0),湖北)
    *            //(20170412,2017041203,List(1.0, 5000.0, 1.0, 36320.0),江苏)
    */
  def insert13(rdd: ((String,String),List[Double]) ): Unit ={
    val provience = rdd._1._2.toString
    val totalOrder = rdd._2(0).toInt
    val totalSuccessOrder = rdd._2(2).toInt

    DB.autoCommit(implicit session => {
      SQL("insert into table13(provience, totalOrder, totalSuccessOrder) values(?,?,?)")
        .bind(provience, totalOrder, totalSuccessOrder)
        .update()
        .apply()
    })
  }




  /**
    * 插入一条数据
    *
    * @param rdd ((String,String),List[Double])
    *            ((时间,省份),List[Double])
    *            //(20170412,2017041203,List(1.0, 5000.0, 1.0, 15203.0),湖北)
    *            //(20170412,2017041203,List(1.0, 5000.0, 1.0, 36320.0),江苏)
    */
  def insert14(rdd: ((String,String),List[Double]) ): Unit ={
    val hour = rdd._1._1.toString
    val totalSuccessOrder = rdd._2(2).toInt
    val money = rdd._2(1).toDouble

    DB.autoCommit(implicit session => {
      SQL("insert into table14(hour, totalSuccessOrder, money) values(?,?,?)")
        .bind(hour, totalSuccessOrder, money)
        .update()
        .apply()
    })
  }




  /**
    * 插入一条数据
    *
    * @param rdd ((String,String),List[Double])
    *            ((时间,省份),List[Double])
    *            //(20170412,2017041203,List(1.0, 5000.0, 1.0, 15203.0),湖北)
    *            //(20170412,2017041203,List(1.0, 5000.0, 1.0, 36320.0),江苏)
    */
  def insert21(rdd: ((String,String),List[Double]) ): Unit ={
    val day = rdd._1._1.toString
    val provience = rdd._1._2.toString
    val totalOrder = rdd._2(0).toInt
    val totalSuccessOrder = rdd._2(2).toInt

    DB.autoCommit(implicit session => {
      SQL("insert into table21(day, provience, totalOrder, totalSuccessOrder) values(?,?,?,?)")
        .bind(day, provience, totalOrder, totalSuccessOrder)
        .update()
        .apply()
    })
  }




  /**
    * 插入一条数据
    *
    * @param rdd ((String,String),List[Double])
    *            ((时间,省份),List[Double])
    * //(20170412,2017041203,201704120300,List(1.0, 5000.0, 1.0, 15203.0),湖北)
    * //(20170412,2017041203,201704120300,List(1.0, 5000.0, 1.0, 36320.0),江苏)
    */
  def insert22(rdd: ((String,String,String,String),List[Double]) ): Unit ={
    val day = rdd._1._1.toString
    val hour = rdd._1._2.toString
    val minute = rdd._1._3.toString

    val provience = rdd._1._4.toString
    val totalOrder = rdd._2(0).toInt
    val totalSuccessOrder = rdd._2(2).toInt
    val money = rdd._2(1).toDouble

    DB.autoCommit(implicit session => {
      SQL("insert into table22(day,hour,minute, provience, totalOrder, totalSuccessOrder, money) values(?,?,?,?,?,?,?)")
        .bind(day,hour,minute, provience, totalOrder, totalSuccessOrder,money )
        .update()
        .apply()
    })
  }
}
