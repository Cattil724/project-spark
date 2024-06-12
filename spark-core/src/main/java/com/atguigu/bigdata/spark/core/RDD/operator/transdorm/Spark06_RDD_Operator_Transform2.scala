package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
//    使用groupBy算子用字符串首字母分区
    val rdd: RDD[String] = sc.makeRDD(List("Hello","Spark","Scala","Hadoop"),2)
    val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))
    groupRDD.collect().foreach(println)
    sc.stop()


  }

}
