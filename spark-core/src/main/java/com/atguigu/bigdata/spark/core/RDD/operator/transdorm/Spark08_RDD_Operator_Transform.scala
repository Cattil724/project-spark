package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    // 抽取数据不放回（伯努利算法）
    // 伯努利算法：又叫 0、1 分布。例如扔硬币，要么正面，要么反面。
    // 具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
    // 第一个参数：抽取的数据是否放回，false：不放回
    // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
    // 第三个参数：随机数种子
    println(rdd.sample(
      true,
      0.3

    ).collect().mkString(","))
    // 抽取数据放回（泊松算法）
    // 第一个参数：抽取的数据是否放回，true：放回；false：不放回
    // 第二个参数：重复数据的几率，范围大于等于 0.表示每一个元素被期望抽取到的次数
    // 第三个参数：随机数种子
    sc.stop()
  }

}
