package com.atguigu.bigdata.spark.core.RDD.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)
    //      读取文件数据都在那个文件内
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("data")
    rdd.collect().foreach(println)
    sc.stop()
  }


}
