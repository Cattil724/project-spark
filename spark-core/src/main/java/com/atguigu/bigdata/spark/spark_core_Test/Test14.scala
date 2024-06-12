package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test14 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("test")
    val sc=new SparkContext(sparkConf)
    //TODO 创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，计算差集，两个都算
    val rdd1: RDD[Int] = sc.makeRDD(1 to 6)
    val rdd2: RDD[Int] = sc.makeRDD(4 to 10)
    rdd1.subtract(rdd2).foreach(println(_))
    rdd2.subtract(rdd1).foreach(println(_))
    sc.stop()
  }

}
