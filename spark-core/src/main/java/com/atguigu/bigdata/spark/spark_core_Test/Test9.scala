package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test9 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("test")
    val sc=new SparkContext(sparkConf)
    //TODO 创建一个 RDD数据为Array(10,10,2,5,3,5,3,6,9,1),对 RDD 中元素执行去重操作
    val rdd: RDD[Int] = sc.makeRDD(Array(10, 10, 2, 5, 3, 5, 3, 6, 9, 1))
    //distinct:去重
    val newRDD: RDD[Int] = rdd.distinct()
    newRDD.foreach(println)
    sc.stop()
  }

}
