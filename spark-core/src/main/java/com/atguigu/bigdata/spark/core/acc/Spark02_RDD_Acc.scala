package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Acc {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//    Acc:系统累加器
//        累加器会把累加后的数据返回Driver
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")
    sc.doubleAccumulator
    sc.collectionAccumulator
    val mapRDD=rdd.map(
      num=>{
//        使用累加器
        sumAcc.add(num)
        num
      }

    )
//    获取累加器值
//    少加：转换算子中调用累加器，如果没有行动算子的话，那么不行执行
//    多加：转换算子中调用累加器，如果多个行动算子的话，那么多次执行
//    一般情况下累加器会置在行动算子进行操作
    mapRDD.collect()
    mapRDD.collect()
    println(sumAcc.value)
  }

}
