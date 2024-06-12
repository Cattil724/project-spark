package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//数据结构：时间戳，省份，城市，用户，广告 ，样本如下，字段使用空格分割
//1516609143867 6 7 64 16
//1516609143869 9 4 75 18
//1516609143869 1 7 87 12
object Test21_2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(sparkConf)
    //TODO 统计出每一个省份广告被点击次数的 TOP3，数据在access.log文件中
    val dataRDD: RDD[String] = sc.textFile("data/Test/access.log")
    val mapRDD: RDD[(String, Int)] = dataRDD.map {
      line =>
        val Array(_, a, _, _, b) = line.split("\\s+")
        (a, b.toInt)
    }
    val reduceRDD=mapRDD.reduceByKey(_+_)
    reduceRDD.sortBy(_._2,false).take(3).foreach(println)
    sc.stop()
  }
}
