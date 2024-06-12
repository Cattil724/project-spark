package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test16 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("test")
    val sc=new SparkContext(sparkConf)
    //TODO 创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，计算 2 个 RDD 的笛卡尔积
    val rdd1: RDD[Int] = sc.makeRDD(1 to 6)
    val rdd2: RDD[Int] = sc.makeRDD(4 to 10)
    rdd1.cartesian(rdd2).foreach(println(_))
  }

}
