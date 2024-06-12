package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val newRDD: RDD[(Int, Int)] = rdd.map((_, 1))
//    将数据按照指定 Partitioner 重新进行分区。Spark 默认的分区器是 HashPartitioner
    newRDD.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")

    sc.stop()
  }

}
