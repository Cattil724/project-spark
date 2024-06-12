package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test17 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("test")
    val sc=new SparkContext(sparkConf)
    //TODO 创建两个RDD，分别为rdd1和rdd2数据分别为1 to 5和11 to 15，对两个RDD拉链操作
    val rdd1: RDD[Int] = sc.makeRDD(1 to 5)
    val rdd2: RDD[Int] = sc.makeRDD(11 to 15)
    rdd1.zip(rdd2).foreach(println(_))
  }

}
