package com.atguigu.bigdata.spark.core.RDD.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spaek01_RDD_Long {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd1= sc.textFile("output1")
    println(rdd1.collect().mkString(","))

    val rdd2 = sc.objectFile[(String,Int)]("output2")
    println(rdd2.collect().mkString(","))

    val rdd3=sc.sequenceFile[String,Int]("output3")
    println(rdd3.collect().mkString(","))
    sc.stop()
  }

}
