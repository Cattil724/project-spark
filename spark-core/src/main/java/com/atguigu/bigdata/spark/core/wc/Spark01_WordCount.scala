package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("WordCount")
    val  sc=new SparkContext(conf)

    val lines = sc.textFile("data/word.txt")

    val words = lines.flatMap(_.split(" "))

    val wordGrouo = words.groupBy(word => word)

    val wordToCount = wordGrouo.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    val array = wordToCount.collect()
    array.foreach(println)
    sc.stop()
  }

}
