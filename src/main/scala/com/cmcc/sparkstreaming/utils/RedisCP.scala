package com.cmcc.sparkstreaming.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisCP {
  val config = new JedisPoolConfig()
  //最大连接数
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //使用borrow方法时，是否进行有效性检查
  config.setTestOnBorrow(true)
  // 配置、ip、端口、超时时长、密码
  val redisPool = new JedisPool(config, "192.168.126.21", 6379, 10000, "123456")

  //获得连接
  def getConn(): Jedis ={
    redisPool.getResource
  }
}
