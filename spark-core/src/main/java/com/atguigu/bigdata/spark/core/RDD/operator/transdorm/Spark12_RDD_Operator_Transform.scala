package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd = sc.makeRDD(List(("1",1),("11",2),("2",3)), 2)
//    sortBy默认升序第二个参数false变成降序
    val newRDD: RDD[(String, Int)] = rdd.sortBy(x => x._1.toInt,false)
    newRDD.collect().foreach(println)
    sc.stop()
  }

}
