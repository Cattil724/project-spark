package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test4 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("test")
    val sc=new SparkContext(sparkConf)
    //TODO 创建一个 4 个分区的 RDD数据为Array(10,20,30,40,50,60)，使用glom将每个分区的数据放到一个数组
    val rdd: RDD[Int] = sc.makeRDD(Array(10, 20, 30, 40, 50, 60),4)
    val newRDD: RDD[Array[Int]] = rdd.glom()
    newRDD.foreach(x=>println(x.mkString("Array(",",",")")))
    sc.stop()
  }

}
