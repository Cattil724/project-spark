package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Acc {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//    Acc:系统累加器
//        累加器会把累加后的数据返回Driver
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")
    sc.doubleAccumulator
    sc.collectionAccumulator
    rdd.foreach(
      num=>{
//        使用累加器
        sumAcc.add(num)

      }

    )
//    获取累加器值
    println(sumAcc.value)
  }

}
