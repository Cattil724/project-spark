package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
//    coalesce的功能是自定分区数量
//    默认修改分区的数量不会打乱数据内容（这样有可能会导致数据倾斜）
//    这样我们可以加一个true，把数据shuffle（洗牌）会完全打乱

    val rdd2: RDD[Int] = rdd.coalesce(2,true)
    rdd2.saveAsTextFile("output")

    sc.stop()
  }

}
