package com.atguigu.bigdata.spark.core.RDD.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Dep {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("WordCount")
    val  sc=new SparkContext(conf)

    val lines = sc.textFile("data/word.txt")
    println(lines.dependencies)
    println("**************************************")
    val words = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("**************************************")
    val wordToCnoe = words.map(
      word => (word, 1)
    )
    println(wordToCnoe.dependencies)
    println("**************************************")
    val wordToCount: RDD[(String, Int)] = wordToCnoe.reduceByKey(_ + _)
    println(wordToCount.dependencies)
    println("**************************************")

    val array = wordToCount.collect()
    array.foreach(println)
    sc.stop()
  }

}
