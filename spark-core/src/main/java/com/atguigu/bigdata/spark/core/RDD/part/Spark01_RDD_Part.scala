package com.atguigu.bigdata.spark.core.RDD.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_Part  {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd=sc.makeRDD(List(
      ("nba","xxxxxx"),
      ("cba","xxxxxx"),
      ("wnba","xxxxxx"),
      ("nba","xxxxxx")
    ),3)
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    partRDD.saveAsTextFile("output")
    sc.stop()
  }
  /*
  自定义分区器
  */
  class MyPartitioner extends Partitioner{
//    分区数量
    override def numPartitions: Int = 3

//    根据key值返回数据所在的分区索引位置（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba"=>0
        case "wnba"=>1
        case _=>2
      }
    }
  }

}
