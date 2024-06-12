package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
//    map读取指定位置的内容
    val mapRDD: RDD[String] = rdd.map(
      line => {
        val datas: Array[String] = line.split(" ")
        datas(6)
      }
    )
    mapRDD.collect().foreach(println)
    sc.stop()


  }

}
