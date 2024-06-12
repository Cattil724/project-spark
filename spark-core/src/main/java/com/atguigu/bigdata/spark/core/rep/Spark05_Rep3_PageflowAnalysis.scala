package com.atguigu.bigdata.spark.core.rep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Rep3_PageflowAnalysis {

//  TODO:Top10 热门品类
      def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(conf)
        //        1.读取原始日志数据
        val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
        val actionDataRDD=actionRDD.map(
          action=>{
            val datas=action.split("_")
            UserVisitAction(
              datas(0),
              datas(1).toLong,
              datas(2),
              datas(3).toLong,
              datas(4),
              datas(5),
              datas(6).toLong,
              datas(7).toLong,
              datas(8),
              datas(9),
              datas(10),
              datas(11),
              datas(12).toLong,
            )
          }
        )
        actionDataRDD.cache()
        //TODO 对指定跳转页面连续跳转进行统计
        val ids=List[Long](1,2,3,4,5,6,7)
        val okflowIds: List[(Long, Long)] = ids.zip(ids.tail)
        //TODO 分母计算
        val pageidToCuntMap: Map[Long, Long] = actionDataRDD.filter(
          action => {
            ids.init.contains(action.page_id)
          }
        ).map(
          action => {
            (action.page_id, 1L)
          }
        ).reduceByKey(_ + _).collect().toMap
        //TODO 分子计算
//        根据session分组
        val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)
        //排序
        val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
          line => {
            //根据动作的时间点
            val sortLine = line.toList.sortBy(_.action_time)
            //根据某个页面的 ID
            val flowIds = sortLine.map(_.page_id)
            //【1，2，3，4】
            //【1-2】【2-3】【3-4】
            //【1，2，3，4】
            //【2，3，4】
            val pageflowIds = flowIds.zip(flowIds.tail)
            pageflowIds.filter(
              t=>{
                okflowIds.contains(t)
              }
            ).map(
              line => (line, 1)
            )
          }
        )
        //((1,2),1)
        val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(line => line)
        //((1,2),sum)
        val dataRDD=flatRDD.reduceByKey(_+_)
        //TODO 计算单跳转换率
        dataRDD.foreach{
          case((pageid1,pageid2),sum)=>{
            val lon: Long = pageidToCuntMap.getOrElse(pageid1, 0L)
            println(s"页面${pageid1}跳转到页面${pageid2}单跳转换率为："+(sum.toDouble/lon))
          }
        }
        sc.stop()
      }
  //用户访问动作表
  case class UserVisitAction(
                              date: String,//用户点击行为的日期
                              user_id: Long,//用户的 ID
                              session_id: String,//Session 的 ID
                              page_id: Long,//某个页面的 ID
                              action_time: String,//动作的时间点
                              search_keyword: String,//用户搜索的关键词
                              click_category_id: Long,//某一个商品品类的 ID
                              click_product_id: Long,//某一个商品的 ID
                              order_category_ids: String,//一次订单中所有品类的 ID 集合
                              order_product_ids: String,//一次订单中所有商品的 ID 集合
                              pay_category_ids: String,//一次支付中所有品类的 ID 集合
                              pay_product_ids: String,//一次支付中所有商品的 ID 集合
                              city_id: Long
                            )
  //城市 id
}