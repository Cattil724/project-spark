package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test3 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("test")
    val sc=new SparkContext(sparkConf)
    //TODO 创建一个元素为 1-5 的RDD，运用 flatMap创建一个新的 RDD，新的 RDD 为原 RDD 每个元素的 平方和三次方 来组成 1,1,4,8,9,27…
    val rdd: RDD[Int] = sc.makeRDD(1 to 5)
    val newRDD: RDD[Int] = rdd.flatMap(x => {
      //Math：数学
      List(Math.pow(x, 2).toInt, Math.pow(x, 3).toInt)
    })
    newRDD.foreach(println(_))
    sc.stop()
  }

}
