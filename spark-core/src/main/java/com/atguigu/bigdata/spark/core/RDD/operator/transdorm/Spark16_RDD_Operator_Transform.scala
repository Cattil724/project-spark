package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
//    groupByKey是固定相同key分组
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))
    val newRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    newRDD.collect.foreach(println)
    sc.stop()
  }

}
