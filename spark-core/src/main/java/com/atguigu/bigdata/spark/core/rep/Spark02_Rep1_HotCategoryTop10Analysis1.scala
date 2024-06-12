package com.atguigu.bigdata.spark.core.rep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Rep1_HotCategoryTop10Analysis1 {

//  TODO:Top10 热门品类
      def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc=new SparkContext(conf)
//        1.读取原始日志数据
        val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
//      Q:原始数据多次复用
//      Q:cogroup有可能存在shuffle降低性能
        actionRDD.cache()
//        2.统计品类的总点击数量：（品类ID，总点击数量）
        val clickActionRDD=actionRDD.filter(
          line=>{
            val datas=line.split("_")
            datas(6) !="-1"
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
              datas(8) !="null"
            }
          )
          val orderCountRDD=orderActionRDD.flatMap(
            line=>{
              val datas=line.split("_")
              val cid=datas(8)
              val cids=cid.split(",")
              cids.map(id=>(id,1))
            }
          ).reduceByKey(_+_)
//        4.统计品类的总支付数量：（品类ID，总支付数量）
          val payActionRDD=actionRDD.filter(
            line=>{
              val datas=line.split("_")
              datas(10) !="null"
            }
          )
        val payCountRDD=payActionRDD.flatMap(
          line=>{
            val data=line.split("_")
            val cid=data(10)
            val cids=cid.split(",")
            cids.map(id=>(id,1))
          }
        ).reduceByKey(_+_)
        /*
        //      Q:cogroup有可能存在shuffle降低性能
        我们可以把数据直接转成（品类ID（总点击数量，0，0））
                             （品类ID（0，总下单数量，0））
                              （品类ID（0，0，总支付数量））聚合
        （品类ID（总点击数量，0，0））+（品类ID（0，总下单数量，0））=>（品类ID（总点击数量，总下单数量，0））
        （品类ID（总点击数量，总下单数量，0））+（品类ID（0，0，总支付数量））=>（品类ID，（总点击数量，总下单数量，总支付数量））
        */
//        5.将品类进行排序，并将取前10名
//          总点击数量排序、总下单数量排序、总支付数量排序
//          （品类ID，（总点击数量，总下单数量，总支付数量））
          val rdd1=clickCountRDD.map{
            case (cid,cnt)=>{
              (cid,(cnt,0,0))
            }
          }
        val rdd2=orderCountRDD.map{
          case (cid,cnt)=>{
            (cid,(0,cnt,0))
          }
        }
        val rdd3=payCountRDD.map{
          case (cid,cnt)=>{
            (cid,(0,0,cnt))
          }
        }
//        把三个数据合并
        val unionRDD=rdd1.union(rdd2).union(rdd3)

//        聚合
          val analysisRDD: RDD[(String, (Int, Int, Int))] =unionRDD.reduceByKey(
            (t1,t2)=>{
              (t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)
            }
          )
        val resultRDD = analysisRDD.sortBy(_._2, false).take(10)
//        打印控制台
//        resultRDD.foreach(println)

        sc.stop()
      }

}
