package com.atguigu.bigdata.spark.core.RDD.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//    合并
    val i: Int = rdd.reduce(_ + _)
    println(i)
//    统计数据个数
    val cnt = rdd.count()
    println(cnt)
//    获取数据第一个数据
    val first=rdd.first()
    println(first)

//    获取N个数据
    val ints: Array[Int] = rdd.take(3)
    println(ints.mkString(","))
//    数据排序后，取N个数据
val rdd1: RDD[Int] = sc.makeRDD(List(4, 3, 2, 1))
    val ints1: Array[Int] = rdd1.takeOrdered(3)
    println(ints1.mkString(","))
    sc.stop()
  }

}
