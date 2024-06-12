package com.atguigu.bigdata.spark.core.rep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Rep2_HotCategoryTop10SessionAnalysis {

//  TODO:Top10 热门品类
      def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc=new SparkContext(conf)
//        1.读取原始日志数据
        val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
/*
        6.2 需求 2：Top10 热门品类中每个品类的 Top10 活跃 Session 统计
        6.2.1 需求说明
        在需求一的基础上，增加每个品类用户 session 的点击统计
*/
        actionRDD.cache()
        val top10Ids: Array[String] = top10Category(actionRDD)
//       过滤数据
        val filterRDD=actionRDD.filter(
          action=>{
            val datas=action.split("_")
            if (datas(6)!="-1"){
              top10Ids.contains(datas(6))
            }else{
              false
            }
          }
        )
         val reduceRDD=filterRDD.map(
          action=>{
            val datas=action.split("_")
            ((datas(6),datas(2)),1)
          }
        ).reduceByKey(_+_)
        val mapRDD=reduceRDD.map{
          case ((cid,sid),sum)=>{
            (cid,(sid,sum))
          }
        }
        val guoupRDD=mapRDD.groupByKey()
        val resultRDD=guoupRDD.mapValues(
          iter=>{
            iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
          }
        )
        resultRDD.collect().foreach(println)
        sc.stop()
      }
      def top10Category(actionRDD:RDD[String]) ={
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
        analysisRDD.sortBy(_._2, false).take(10).map(_._1)
      }

}