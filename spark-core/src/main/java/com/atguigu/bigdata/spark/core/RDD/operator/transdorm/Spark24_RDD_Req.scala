package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Req {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
//    获取原数据：时间戳，省份，城市，用户，广告
    val dataRDD: RDD[String] = sc.textFile("data/agent.log")
//    把需要得数据取出并转换
//    =>
//    ((省份，广告)，1)
    val mapRDD=dataRDD.map(
      line=>{
        val datas=line.split(" ")
        ((datas(1),datas(4)),1)
      }
    )
//    把点击得广告次数聚合
//    =>
//    ((省份，广告)，sum)

    val reduRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)
    //    聚合的结果进行结构转换
    //    =>
    //    (省份,(广告，sum))
    val newMapRDD=reduRDD.map{
      case((prv,ad),sum)=>{
        (prv,(ad,sum))
      }
    }
//    将转换后的数据进行省份分分组
//    =>
//    (省份,【(广告A，sum),(广告B，sum)】)
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()
//   分组完成后数据组内排序（降序）显示前三名
    val resultRDD=groupRDD.mapValues(
      iter=>{
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
//    采集数据显示在控制台
    resultRDD.collect().foreach(println)
    sc.stop()
  }

}
