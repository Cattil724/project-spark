package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
//    map
//    rdd.map((num:Int)=>{num*2})
//    rdd.map((num:Int)=>num*2)
//    rdd.map((num)=>num*2)
//    rdd.map(num=>num*2)
    val mapRDD: RDD[Int] = rdd.map(_ * 2)
    mapRDD.collect().foreach(println)
    sc.stop()


  }

}
