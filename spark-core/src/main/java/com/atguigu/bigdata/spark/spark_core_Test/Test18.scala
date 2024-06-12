package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test18 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(sparkConf)
    //TODO 创建一个RDD数据为List((“female”,1),(“male”,5),(“female”,5),(“male”,2))，请计算出female和male的总数分别为多少
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("female", 1), ("male", 5), ("female", 5), ("male", 2)))
    rdd.reduceByKey(_+_).foreach(println(_))


    sc.stop()
  }
}
