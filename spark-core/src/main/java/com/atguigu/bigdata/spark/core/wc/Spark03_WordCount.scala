package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("WordCount")
    val  sc=new SparkContext(conf)

    val lines = sc.textFile("data")

    val words = lines.flatMap(_.split(" "))

    val wordToCnoe = words.map(
      word => (word, 1)
    )

    val wordToCount: RDD[(String, Int)] = wordToCnoe.reduceByKey(_ + _)


    val array = wordToCount.collect()
    array.foreach(println)
    sc.stop()
  }

}
