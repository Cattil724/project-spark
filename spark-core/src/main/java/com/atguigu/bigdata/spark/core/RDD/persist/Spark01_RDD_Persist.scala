package com.atguigu.bigdata.spark.core.RDD.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("Persist")
    val  sc=new SparkContext(conf)

    val list=List("Hello Scala","Hello Spark")
    val rdd: RDD[String] = sc.makeRDD(list)
    val flatRDD=rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map(wrod=>{
      println("@@@@@@@@@@@@@@@@@@@@@@@")
      (wrod, 1)
    }
      )
//    cache：是缓存
//          RDD不能存放数据 如果想实现两个功能那他会重复读取两遍mapRDD，性能降低
//          可以把mapRDD的数据放到缓存中，或者文件当中这样提高性能
//    mapRDD.cache()
//    persist:磁盘文件
//    持久化操作必须在行动算子执行时完成的
    mapRDD.persist(StorageLevel.DISK_ONLY)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("*************************")
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)
  }


}
