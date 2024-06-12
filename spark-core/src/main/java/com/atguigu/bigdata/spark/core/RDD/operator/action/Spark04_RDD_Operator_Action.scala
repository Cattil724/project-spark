package com.atguigu.bigdata.spark.core.RDD.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 1, 4))
    val rdd= sc.makeRDD(List(
      ("a",1),("a",2),("a",3))
    )
//    统计出现的次数
//    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
//    println(intToLong)

//    统计key出现的次数
val stringToLong: collection.Map[String, Long] = rdd.countByKey()
    println(stringToLong)
    sc.stop()
  }

}
