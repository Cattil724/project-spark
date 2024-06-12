package com.atguigu.bigdata.spark.core.wc

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.OL
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}





object Spark04_WordCount  {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val  sc=new SparkContext(conf)
    wordCount5(sc)
    sc.stop()
  }
//  groupBy
  def wordCount1(sc : SparkContext): Unit ={
    val rdd=sc.makeRDD(List("hello scala","hadoop spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    /*
    (scala,CompactBuffer(scala))
    (spark,CompactBuffer(spark))
    (hadoop,CompactBuffer(hadoop))
    (hello,CompactBuffer(hello))
    */
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
    /*
    (scala,1)
    (spark,1)
    (hadoop,1)
    (hello,1)
    */

  }
//  groupByKey
  def wordCount2(sc : SparkContext): Unit ={
    val rdd=sc.makeRDD(List("hello scala","hadoop spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
    /*
    (scala,CompactBuffer(1))
    (spark,CompactBuffer(1))
    (hadoop,CompactBuffer(1))
    (hello,CompactBuffer(1))
    */

    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }
//  reduceByKey
  def wordCount3(sc : SparkContext): Unit ={
    val rdd=sc.makeRDD(List("hello scala","hadoop spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
    /*
    (scala,1)
    (spark,1)
    (hadoop,1)
    (hello,1)
    */
    wordCount.foreach(println)
  }
//  aggregateByKey
  def wordCount4(sc : SparkContext): Unit ={
    val rdd=sc.makeRDD(List("hello scala","hadoop spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_ + _, _ + _)
  }
//  combineByKey
  def wordCount5(sc : SparkContext): Unit ={
    val rdd=sc.makeRDD(List("hello scala","hadoop spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.combineByKey(
      v=>v,
      (x:Int,y)=>x+y,
      (x:Int,y:Int)=>x+y
    )

  }
//  foldByKey
  def wordCount6(sc : SparkContext): Unit ={
    val rdd=sc.makeRDD(List("hello scala","hadoop spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_ + _)
  }
//  countByKey
  def wordCount7(sc : SparkContext): Unit ={
    val rdd=sc.makeRDD(List("hello scala","hadoop spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: collection.Map[String, Long] = wordOne.countByKey()
  }
//  countByValue
  def wordCount8(sc : SparkContext): Unit ={
    val rdd=sc.makeRDD(List("hello scala","hadoop spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val wordCount: collection.Map[String, Long] = words.countByValue()
  }



}
