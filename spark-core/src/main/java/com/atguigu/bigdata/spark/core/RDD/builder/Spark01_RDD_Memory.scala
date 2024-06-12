package com.atguigu.bigdata.spark.core.RDD.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)
    val seq = Seq(1, 2, 3, 4)
    //    在内存中创建
    val rdd: RDD[Int] = sc.makeRDD(seq)
    rdd.collect().foreach(print)
    sc.stop()
  }


}
