package com.atguigu.bigdata.spark.core.RDD.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
//  aggregateByKey:初始值不参与分区间运算
//    aggregate：初始值参与分区间运算
    //    10+1+3+10,3+4+10 ==40
    val regate: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(regate)
    val fold1=rdd.fold(10)(_+_)
    println(fold1)
    sc.stop()
  }

}
