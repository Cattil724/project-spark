package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("test")
    val sc=new SparkContext(sparkConf)
    //TODO 创建一个10-20数组的RDD，使用mapPartitions将所有元素*2形成新的RDD
    val rdd =sc.makeRDD(10 to 20)
    val mapRDD = rdd.mapPartitions(lint => {
      lint.map(_ * 2)
    })
    mapRDD.foreach(println(_))
    sc.stop()
  }

}
