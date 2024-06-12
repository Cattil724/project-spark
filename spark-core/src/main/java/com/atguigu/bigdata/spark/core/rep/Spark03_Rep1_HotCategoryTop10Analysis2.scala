package com.atguigu.bigdata.spark.core.rep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Rep1_HotCategoryTop10Analysis2 {

//  TODO:Top10 热门品类
      def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc=new SparkContext(conf)
//        1.读取原始日志数据
        val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
//        把数据转换架构
//        点击量  （品类ID（1，0，0））
//        下单量  （品类ID（0，1，0））
//        支付量  （品类ID（0，0，1））
        val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
          line => {
            val datas = line.split("_")
            if (datas(6) != "-1") {
//              点击场景
              List((datas(6), (1, 0, 0)))
            } else if (datas(8) != "null") {
//              下单场景
              val id = datas(8).split(",")
              id.map(id => (id, (0, 1, 0)))
            } else if (datas(10) != "null") {
//              支付场景
              val id = datas(10).split(",")
              id.map(id => (id, (0, 0, 1)))
            } else {
              Nil
            }
          }
        )
//        聚合成（品类ID，（总点击量，总下单量，总支付量））
        val analysisRDD=flatRDD.reduceByKey(
          (t1,t2)=>{
            (t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)
          }
        )
//        排序取前10名
        val resultRDD = analysisRDD.sortBy(_._2, false).take(10)
//        打印控制台1
        resultRDD.foreach(println)

        sc.stop()
      }

}