package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("test")
    val sc=new SparkContext(sparkConf)
    //TODO 创建一个1-10数组的RDD，将所有元素*2形成新的RDD
    val RDD:RDD[Int]= sc.makeRDD(1 to 10)
    val newRDD: RDD[Int] = RDD.map(_ * 2)
    newRDD.collect().foreach(x=>print(x+","))

//    val listRDD: RDD[Int] = sc.makeRDD(List(1 to 10))
//    listRDD.map(num=>num*2)
//    listRDD.map(_*2)

    sc.stop()
  }

}
