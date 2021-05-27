package com.ldg.gues

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

object OnlieAnaiysis {

  def main(args: Array[String]): Unit = {
    //得到SparkConf对像
    val conf = new SparkConf().setMaster("local[*]").setAppName("OnlineAnaiysis")
    // 得到SparkContext对像
    val sc = new SparkContext(conf)


    //读到HDFS系统中的文件
    val data = sc.textFile(args(0))

    // -1. 数据清洗  考滤无效数据 imei没有的, null  存放 cleanMap表中
    println("剔除type等于3的数据 imei为Unknown 为\"\" 为\"000000000000000\"的数据")
    val notContainsType3 = data.filter(!_.contains("\\\"type\\\":\\\"3\\\""))
      .filter(!_.contains("\\\"imei\\\":\\\"\\\""))
      .filter(!_.contains("000000000000000"))
      .filter(!_.contains("Unknown"))

    println("过滤logid或imei不存在的数据 \\\"imei\\\":\\\"\\\"")
    val cleanData = notContainsType3.filter(_.contains("logid")).filter(_.contains("imei"))
    cleanData.cache()

    // 0.清理数据入mysql库cleanMap表
    cleanMapIntoMysql(cleanData)

    //1 .按渠道分类统计总量入mysql库sum表
    sumMapIntoMysql(cleanData)

    //2. 按区域和渠道分类统计 登陆电信用的数据量 入mysql库a_c_map表
    areacodeAndchannelnoMapIntoMysql(cleanData)

    //3. 按区域和请求方式分类统计登陆电信用的数据量 入mysql库a_r_map表
    areacodeAndrequesttypeMapIntoMysql(cleanData)

    //4 统计分用户 一共登陆多少次 首次登陆时间是多少，及在线时长 入mysql库userInfoMap表
    userInfoMapIntoMysql(cleanData)



    //停止
    sc.stop()

  }


  //统计用户 一共登陆多少次 首次登陆时间是多少，及在线时长
  def userInfoMapIntoMysql(cleanData: RDD[String]): Unit = {
    val userInfoMap = cleanData.map {
      line =>
        val data = formatLine(line).split(",")
        //用户   登陆时间    时长
        (data(0), data(7), data(6))
                              //用户   总次数            登陆时间                   时长
    }.groupBy(_._1).map(x => (x._1, x._2.size, x._2.map(y => y._2).min, x._2.map(y => y._3.toLong).sum))

    println("打印出Map4数据：")
    userInfoMap.collect().foreach(println)

    userInfoMap.foreachPartition(MysqlUtil.add_table_userInfoMap)
    println("按区域和渠道分类统计 登陆电信用的数据总量入mysql库完成!")

  }

  //按[区域和渠道]分类统计 登陆电信用的数据量
  def areacodeAndchannelnoMapIntoMysql(cleanData: RDD[String]): Unit = {
    val areacodeAndchannelnoMap = cleanData.map {
      line =>
        val data = formatLine(line).split(",")
        (data(3), data(5))
      //区域         渠道      数量
    }.groupBy(x => (x._1, x._2)).map(x => (x._1._1, x._1._2, x._2.size))

    println("打印出areacodeAndchannelnoMap数据：")
    areacodeAndchannelnoMap.collect().foreach(println)

    areacodeAndchannelnoMap.foreachPartition(MysqlUtil.add_table_areacodeAndchannelnoMap)
    println("按区域和渠道分类统计 登陆电信用的数据总量入mysql库完成!")

  }


  //按[区域,请求方式]分类统计登陆电信用的数据量
  def areacodeAndrequesttypeMapIntoMysql(cleanData: RDD[String]): Unit = {
    val areacodeAndrequesttypeMap = cleanData.map {
      line =>
        val data = formatLine(line).split(",")
        (data(3), data(4))
      //区域         请求类型     数量
    }.groupBy(x => (x._1, x._2)).map(x => (x._1._1, x._1._2, x._2.size))

    println("打印出 按 区域 和 请求方式 分类统计 数据：")
    areacodeAndrequesttypeMap.collect().foreach(println)

    areacodeAndrequesttypeMap.foreachPartition(MysqlUtil.add_table_areacodeAndrequesttypeMap)
    println("按区域和请求方式分类统计 登陆电信用的数据总量入mysql库完成!")

  }

  //按渠道分类统计总量入mysql库
  def sumMapIntoMysql(cleanData: RDD[String]): Unit = {
    val sumMap = cleanData.map {
      line =>
        val data = formatLine(line).split(",")
        (data(5), 1)
    }.reduceByKey(_ + _)
    println("打印出sumMap数据：")
    sumMap.collect().foreach(println)
    sumMap.foreachPartition(MysqlUtil.addsum)
    println("按渠道分类统计总量入mysql库完成!")

  }


  //清理数据入mysql库
  def cleanMapIntoMysql(cleanData: RDD[String]): Unit = {
    //抽取有用的数据信息
    val cleanMap = cleanData.map {
      line =>
        val data = formatLine(line).split(",")
        (data(0), data(1), data(2), data(3), data(4), data(5), data(6), data(7))
    }
    println("打印出cleanMap数据：")
    cleanMap.collect().foreach(println)
    println("明细数据插入数据库")
    cleanMap.foreachPartition(MysqlUtil.cleanMap)
    println("明细数据插入数据库_结束")


  }


  //0用户编号imei（A0001~A1000）
  //1登录唯一ID   logid  同一用户有多个登陆唯一ID
  //2IP地址requestip（192.168.0.1、192.168.0.2、192.168.0.3）
  //3区域areacode（重庆，南岸）
  //4请求类型requesttype（0:GET；1:POST）
  //5渠道channelno（0:app；1:PC机；2:平板电脑）
  //6时长datatime
  //7登陆时间logingtime7登陆时间logingtime
  def formatLine(line: String): String = {
    val imeiRegex = """\\"imei\\":\\"([A-Za-z0-9]+)\\"""".r //12345678900987654321
    val logIdRegex =
      """"logid":"([A-Za-z0-9]+)",""".r //201803192035079865882995
    val requestipRegex =
      """"requestip":"([0-9.]+)",""".r
    val areacodeRegex = """"areacode":"([0-9]+)",""".r
    val requesttypeRegex = """"requesttype":"([0-9]+)",""".r
    val channelnoRegex = """\\"channelno\\":\\"([0-9]+)\\"""".r
    val responsedataRegex = """\\"responsedata\\":\\"([0-9]+)\\"""".r
    val datatimeRegex = """"datatime":"([0-9]+)",""".r
    val logingtimeRegex = """"requesttime":"([ :\-0-9]+)",""".r
    val logId = getDataByPattern(logIdRegex, line)
    val requestip = getDataByPattern(requestipRegex, line)
    val areacode = getDataByPattern(areacodeRegex, line)
    val requesttype = getDataByPattern(requesttypeRegex, line)
    val imei = getDataByPattern(imeiRegex, line)
    val channelno = getDataByPattern(channelnoRegex, line)
    val responsedata = getDataByPattern(responsedataRegex, line)
    val datatime = getDataByPattern(datatimeRegex, line)
    val logingtime = getDataByPattern(logingtimeRegex, line)
    //输出数据
    imei + "," + logId + "," + requestip + "," + areacode + "," + requesttype + "," + channelno + "," + datatime + "," + logingtime
  }

  /* 根据正则表达式,查找相应值*/
  def getDataByPattern(p: Regex, line: String): String = {
    val result = (p.findFirstMatchIn(line)).map(item => {
      val s = item group 1 //返回匹配上正则的第一个字符串。
      s
    })
    result.getOrElse("NULL")
  }


}
