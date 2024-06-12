package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)),2)
//    aggregateByKey存在柯里化，有两个参数列表
//    第一个参数表：需要传递一个参数，表示为初值
//                    主要用于当碰见第一个key的时候，和Value进行分区内操作
//    第二个参数表：有两个参数 “一个是分区内进行的计算操作”，
    //             “第二个是分区间计算的操作”
    rdd.aggregateByKey(0)(
      (x,y)=>math.max(x,y),
      (x,y)=>x+y
    ).collect.foreach(println)
    sc.stop()
  }

}
