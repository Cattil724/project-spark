package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4) )
//    使用电脑核的默认分区完成（index，iter）这样的操作
    var mapRDD =rdd.mapPartitionsWithIndex(
      (index,iter)=>{
        iter.map(
          num=>(index,num)
        )
      }

    )
    mapRDD.collect().foreach(println)
    sc.stop()


  }

}
