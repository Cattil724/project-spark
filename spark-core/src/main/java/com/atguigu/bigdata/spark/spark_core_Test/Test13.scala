package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test13 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("test")
    val sc=new SparkContext(sparkConf)
    //TODO 创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，求并集
    val rdd1: RDD[Int] = sc.makeRDD(1 to 6)
    val rdd2: RDD[Int] = sc.makeRDD(4 to 10)

    val rdd3: RDD[Int] = rdd1.intersection(rdd2,1)
//    val rdd4=rdd3.sortBy(x=>x,false)
    rdd3.foreach(println)
    sc.stop()
  }

}
