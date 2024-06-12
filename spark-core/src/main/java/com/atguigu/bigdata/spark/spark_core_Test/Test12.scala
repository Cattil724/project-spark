package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test12 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("test")
    val sc=new SparkContext(sparkConf)
    //TODO 创建一个 RDD数据为1,3,4,10,4,6,9,20,30,16,请给RDD进行分别进行升序和降序排列
    val rdd: RDD[Int] = sc.makeRDD(List(1, 3, 4, 10, 4, 6, 9, 20, 30, 16))
    rdd.sortBy(x=>x).foreach(println)
    rdd.sortBy(x=>x,false).foreach(print(_))
    sc.stop()
  }

}
