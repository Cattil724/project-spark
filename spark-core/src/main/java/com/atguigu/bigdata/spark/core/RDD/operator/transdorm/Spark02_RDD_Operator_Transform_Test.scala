package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
//    取最大值
    //iterator:迭代器
    var mapRDD=rdd.mapPartitions(
      iter=>{
        List(iter.max).iterator
      }
    )
    mapRDD.collect().foreach(println)
    sc.stop()


  }

}
