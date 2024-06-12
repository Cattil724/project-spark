package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
//    RDD-算子 - -mapPartitions 有缓存的功能
    val mpRDD: RDD[Int] = rdd.mapPartitions(
      tier => {
        println(">>>>>>>>>")
        tier.map(_ * 2)
      }
    )

    mpRDD.collect().foreach(println)
    sc.stop()


  }

}
