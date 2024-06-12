package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("WordCount")
    val  sc=new SparkContext(conf)

    val lines = sc.textFile("data/word.txt")

    val words = lines.flatMap(_.split(" "))

    val wordToCnoe = words.map(
      word => (word, 1)
    )
    val wordGrouo: RDD[(String, Iterable[(String, Int)])] = wordToCnoe.groupBy(
      t => t._1
    )
    val wordToCount: RDD[(String, Int)] = wordGrouo.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }


    val array = wordToCount.collect()
    array.foreach(println)
    sc.stop()
  }

}
