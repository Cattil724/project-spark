package com.atguigu.bigdata.spark.spark_core_Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test8 {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("test")
    val sc=new SparkContext(sparkConf)
    //TODO 创建一个 RDD数据为1 to 10，请使用sample放回抽样
    val rdd: RDD[Int] = sc.makeRDD(1 to 10)
    /*
    def sample(
    withReplacement: Boolean, // 抽取的数据是否放回，false：不放回
    // false：抽取的机率，范围[0,1]之间,0=全不取，1=全取
    // true：重复数据的机率，范围大于等于0.表示每个元素期望被抽取到的次数
    fraction: Double, // 抽取的几率
    seed: Long = Utils.random.nextLong // 随机数种子
    ): RDD[T]
*/
    val newRDD: RDD[Int] = rdd.sample(withReplacement = true, 0.5)
    newRDD.foreach(println)
    sc.stop()
  }

}
