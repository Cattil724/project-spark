package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
//    使用flatMap把内部用模式匹配的方式把不同类型的数据内容取出
    val rdd: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))
    val flatRDD=rdd.flatMap(
      data=>{
        data match {
          case list:List[_]=>list
          case dat=>List(dat)
        }
      }
    )
    flatRDD.collect().foreach(println)
    sc.stop()


  }

}
