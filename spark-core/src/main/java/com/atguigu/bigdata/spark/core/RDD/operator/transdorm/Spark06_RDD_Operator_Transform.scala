package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
//    使用groupBy算子把计算的值分成一个分区或者一个组中
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    def groupFasgion(num: Int): Int ={
      num%2
    }
    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFasgion)
    groupRDD.collect().foreach(println)
    sc.stop()


  }

}
