package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("data/apache.log")
    rdd.filter(
      line=>{
        val data=line.split(" ")
        var itme=data(3)
        itme.startsWith("17/05/2015")
      }
    ).collect().foreach(println)
    sc.stop()
  }

}
