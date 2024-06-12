package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3),
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3),
    ))
//    join:是根据相同的key进行join如果key的位置不同也不影响结果
//         如果没有相同key那结果不会出现
//         如果有多个相同得key那会以此匹配会导致笛卡尔积性，结果会多次出现会导致内存受到影响降低性能
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    joinRDD.collect().foreach(println)
    sc.stop()
  }

}
