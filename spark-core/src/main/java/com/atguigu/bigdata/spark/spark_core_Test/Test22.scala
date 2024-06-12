package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//数据结构：时间戳，省份，城市，用户，广告 ，样本如下，字段使用空格分割
//1516609143867 6 7 64 16
//1516609143869 9 4 75 18
//1516609143869 1 7 87 12
object Test22 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(sparkConf)
    //TODO 读取本地文件word.txt,统计出每个单词的个数，保存数据到 hdfs 上
    val dataRDD: RDD[String] = sc.textFile("data/word.txt")
    val mapRDD= dataRDD.flatMap(
      line => line.split("\\s+")
    )
      .map(x=>(x,1))
      .saveAsTextFile("hdfs://node1:8020/wordsOutput")

  }
}
