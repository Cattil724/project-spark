package com.atguigu.bigdata.spark.core.RDD.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd= sc.makeRDD(List(1, 2, 3, 4))
    rdd.collect().foreach(println)
    println("*******************************************")
    rdd.foreach(println)
    sc.stop()
  }

}
