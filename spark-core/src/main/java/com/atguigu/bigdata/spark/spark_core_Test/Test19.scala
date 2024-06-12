package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test19 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(sparkConf)
    //TODO 创建一个有两个分区的 RDD数据为List((“a”,3),(“a”,2),(“c”,4),(“b”,3),(“c”,6),(“c”,8))，取出每个分区相同key对应值的最大值，然后相加
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    rdd.aggregateByKey(0)(
      (tmp, item) => {
        println(tmp,item,"---")
        Math.max(tmp, item)
      },
      (tmp, result) => {
        println(tmp,result,"--")
        tmp + result
      }
    ).foreach(println(_))




    sc.stop()
  }
}
