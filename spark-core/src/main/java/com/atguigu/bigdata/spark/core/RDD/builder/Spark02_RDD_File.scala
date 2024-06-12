package com.atguigu.bigdata.spark.core.RDD.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)
    //      读取文件数据绝对路径也可以用相对路径
    //      val rdd: RDD[String] = sc.textFile("D:\\IdeaProjects\\atguigu-classes\\data\\1.txt")
    //      val rdd: RDD[String] = sc.textFile("data/1.txt")
    //    rdd.collect().foreach(println)
    //      还可以用*号
    //    默认是idea环境的路径
    //    sc.textFile("HDFS://node1:8020/text.txt")
    sc.stop()
  }


}
