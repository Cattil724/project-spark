package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
//    用分区索引取到想要得到那个分区的内容
    var mpRDD=rdd.mapPartitionsWithIndex(
      (index,iter)=>{
        if(index==1){
          iter
        }
        else {
          Nil.iterator
        }
      }
    )
    mpRDD.collect().foreach(println)
    sc.stop()


  }

}
