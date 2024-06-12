package com.atguigu.bigdata.spark.core.RDD.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1),
        ("b", 2),
        ("c", 3)
      )
    )
    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")
  }

}
