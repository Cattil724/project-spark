package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Rep1_HotCategoryTop10Analysis_Test {

//  TODO:Top10 热门品类
      def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc=new SparkContext(conf)
//        1.读取原始日志数据
          val actionRDD=sc.textFile("data/user_visit_action.txt")
//        2.统计品类的总点击数量：（品类ID，总点击数量）
          val clickActionRDD=actionRDD.filter(
            line=>{
              val datas=line.split("_")
              datas(6)!="-1"
            }
          )
        val clickCountRDD=clickActionRDD.map(
          line=>{
            val datas=line.split("_")
            ((datas(6),1))
          }
        ).reduceByKey(_+_)
//        3.统计品类的总下单数量：（品类ID，总下单数量）
            val orderActionRDD=actionRDD.filter(
              line=>{
                val datas=line.split("_")
                datas(8)!="null"
              }
            )
        val orderCountRDD=orderActionRDD.flatMap(
          line=>{
            val datas=line.split("_")
            val cid=datas(8).split(",")
            cid.map(id=>(id,1))
          }
        ).reduceByKey(_+_)
//        4.统计品类的总支付数量：（品类ID，总支付数量）
          val payActionRDD=actionRDD.filter(
            line=>{
              val datas=line.split("_")
              datas(10)!="null"
            }
          )
        val payCountRDD=payActionRDD.flatMap(
          line=>{
            val datas=line.split("_")
            val cid=datas(10).split(",")
            cid.map(id=>(id,1))
          }
        ).reduceByKey(_+_)
//        5.将品类进行排序，并将取前10名
//          总点击数量排序、总下单数量排序、总支付数量排序
//          （品类ID，（总点击数量，总下单数量，总支付数量））
        val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
            clickCountRDD.cogroup(orderCountRDD, payCountRDD)
            val analysisRDD=cogroupRDD.mapValues{
              case (clickiter,orderiter,payiter)=>{
                var clickCnt=0
                val iter1=clickiter.iterator
                if (iter1.hasNext){
                  clickCnt=iter1.next()
                }
                var orderCnt=0
                val iter2=orderiter.iterator
                if (iter2.hasNext){
                  orderCnt=iter2.next()
                }
                var payiCnt=0
                val iter3=payiter.iterator
                if (iter3.hasNext){
                  payiCnt=iter3.next()
                }
                (clickCnt,orderCnt,payiCnt)
              }
            }
        val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
        //        打印控制台
          resultRDD.foreach(println)

        sc.stop()
      }

}
