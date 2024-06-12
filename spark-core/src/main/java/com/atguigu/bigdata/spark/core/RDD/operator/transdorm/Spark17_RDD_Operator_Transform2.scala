package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4),
                                                ("b", 4), ("b", 5), ("b", 6), ("a",7)
    ),2)
//    如果聚合计算时计算相同可以使用以下简化操作
      rdd.foldByKey(0)(_+_).collect.foreach(println)
    sc.stop()
  }

}
