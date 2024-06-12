package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test5 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("test")
    val sc=new SparkContext(sparkConf)
    //TODO 创建一个 RDD数据为Array(1, 3, 4, 20, 4, 5, 8)，按照元素的奇偶性进行分组
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 3, 4, 20, 4, 5, 8))
    val newRDD: RDD[(Boolean, Iterable[Int])] = rdd.groupBy(x => x % 2 == 0)
    newRDD.foreach(println)
    sc.stop()
  }

}
