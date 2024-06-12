package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2)//, ("c", 3),
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3),
    ))
//    左外连接
//    val leftRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
//    leftRDD.collect().foreach(println)
//    右外连接
      val rightRDD: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)
      rightRDD.collect().foreach(println)
    sc.stop()
  }

}
