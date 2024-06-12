package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2)//, ("c", 3),
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3),("c", 6)
    ))
//    cogroup(分组，连接)
    val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    cgRDD.collect().foreach(println)
    sc.stop()
  }

}
