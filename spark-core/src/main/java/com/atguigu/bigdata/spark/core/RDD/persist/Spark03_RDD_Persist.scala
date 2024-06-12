package com.atguigu.bigdata.spark.core.RDD.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist {
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
//      checkpoint 需要落盘 需要指定检查点保存路径
//    检查点路径保存的文件不会，在任务结束后删除数据
//    一般保存路径是以分布式存储系统：HDFS
    mapRDD.checkpoint()
    val reduceRDD= mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("*************************")
    val groupRDD= mapRDD.groupByKey()
    groupRDD.collect().foreach(println)
  }


}
