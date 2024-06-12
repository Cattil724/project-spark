package com.atguigu.bigdata.spark.core.RDD.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 1, 4))
    val rdd= sc.makeRDD(List(
      ("a",1),("a",2),("a",3))
    )
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
//    这个方法存储的数据必须为K--V类型的
    rdd.saveAsSequenceFile("output2")
    sc.stop()
  }

}
