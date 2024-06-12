package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
//    把两个分区的最大值相加操作
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    val mapRDD: RDD[Int] = glomRDD.map(
      list => list.max
    )
    println(mapRDD.collect().sum)
    sc.stop()


  }

}
