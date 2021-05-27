import org.apache.spark.{SparkConf, SparkContext}

object TestSpark {

  def main(args: Array[String]): Unit = {

    //得到SparkConf对像
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    // 得到SparkContext对像
    val sc = new SparkContext(conf)
    val rdd=sc.parallelize(1 to 9,4)
//    val rdd2=rdd.mapPartitions(f=> {
//      var reset =List[(Int,Int,Int)]()
//      var a=f.next()
//      while (f.hasNext)
//        {
//
//          val b=f.next()
//          reset ::=(a,b,TaskContext.get.partitionId)
//          a=b
//
//        }
//      reset.iterator
//    })
//    rdd2.collect().foreach(println)

  }

}
