package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
//    使用glom算子把数据整合成数组
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    glomRDD.collect().foreach(data=>println(data.mkString(",")))
    sc.stop()


  }

}
