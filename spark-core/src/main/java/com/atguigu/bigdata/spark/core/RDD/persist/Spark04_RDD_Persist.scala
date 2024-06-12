package com.atguigu.bigdata.spark.core.RDD.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("Persist")
    val  sc=new SparkContext(conf)
    sc.setCheckpointDir("cp")

    val list=List("Hello Scala","Hello Spark")
    val rdd: RDD[String] = sc.makeRDD(list)
    val flatRDD=rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map(wrod=>{
      println("@@@@@@@@@@@@@@@@@@@@@@@")
      (wrod, 1)
    }
      )
//    checkpoint:它会重复调用数据多次执行，需要联合cache使用
    mapRDD.cache()
    mapRDD.checkpoint()
    val reduceRDD= mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("*************************")
    val groupRDD= mapRDD.groupByKey()
    groupRDD.collect().foreach(println)
  }


}
