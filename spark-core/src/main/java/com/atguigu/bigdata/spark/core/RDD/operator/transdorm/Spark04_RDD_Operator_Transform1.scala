package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
//    使用flatMap把字符串的每一个单词分割
    val rdd: RDD[String] = sc.makeRDD(List(
      "Hello Spark", "Hello Hadoop")
    )
    val flatRDD: RDD[String] = rdd.flatMap(
      list => list.split(" ")
    )
    flatRDD.collect().foreach(println)
    sc.stop()


  }

}
