package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test10 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("test")
    val sc=new SparkContext(sparkConf)
    //TODO 创建一个分区数为5的 RDD，数据为0 to 100，之后使用coalesce再重新减少分区的数量至 2
    val rdd: RDD[Int] = sc.makeRDD(0 to 100,5)
    val newRDD: RDD[Int] = rdd.coalesce(2)
    newRDD.saveAsTextFile("puotin")
    sc.stop()
  }

}
