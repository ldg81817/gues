package com.ldg.gues

import org.apache.spark.{SparkConf, SparkContext}

object wordcount {

  def main(args: Array[String]): Unit = {
    //得到SparkConf对像
     val  conf= new SparkConf().setMaster("local").setAppName("wrodcount")
      // 得到SparkContext对像
      val sc=new SparkContext(conf)

    //读到HDFS系统中的文件
    val rdd1= sc.textFile("hdfs://node1:8020/test/A/B/a.txt")

    //对每一行数据进行map
   val   rdd2=rdd1.flatMap( _.split(" "))

    //构建(k,v)
    val rdd3=rdd2.map( (_,1) )

    //key相同的进行求和
   val rdd4=rdd3.reduceByKey(_+_)
    //输出
     rdd4.collect.foreach(println)

    //停止
    sc.stop()

  }

}
