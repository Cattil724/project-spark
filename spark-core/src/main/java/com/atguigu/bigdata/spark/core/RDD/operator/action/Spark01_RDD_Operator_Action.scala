package com.atguigu.bigdata.spark.core.RDD.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//    TODO - 行动算子
//    行动算子就是触发作业（Job）执行方法
//    底层会创建（ActiveJob）并提交执行
    rdd.collect()
  }

}
