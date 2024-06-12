package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test20 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(sparkConf)
    //TODO 创建一个有两个分区的 pairRDD数据为Array((“a”, 88), (“b”, 95), (“a”, 91), (“b”, 93), (“a”, 95), (“b”, 98))，根据 key 计算每种 key 的value的平均值
    val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
    rdd.groupByKey()
      .map(x=>x._2.sum/x._1.size)
      .foreach(println)

    sc.stop()
  }
}
