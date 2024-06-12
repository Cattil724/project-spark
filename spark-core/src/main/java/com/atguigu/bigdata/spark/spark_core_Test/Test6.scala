package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test6 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("test")
    val sc=new SparkContext(sparkConf)
    //TODO 创建一个 RDD（由字符串组成）Array(“xiaoli”, “laoli”, “laowang”, “xiaocang”, “xiaojing”, “xiaokong”)，过滤出一个新 RDD（包含“xiao”子串）
    val rdd: RDD[String] = sc.makeRDD(Array("xiaoli", "laoli", "laowang", "xiaocang", "xiaojing", "xiaokong"))
    val newRDD: RDD[String] = rdd.filter(_.contains("xiao") )
    newRDD.foreach(println)
    sc.stop()
  }

}
