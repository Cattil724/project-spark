package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
//      扩大分区
val rdd2: RDD[Int] = rdd.repartition(3)
    rdd2.saveAsTextFile("output")

    sc.stop()
  }

}
