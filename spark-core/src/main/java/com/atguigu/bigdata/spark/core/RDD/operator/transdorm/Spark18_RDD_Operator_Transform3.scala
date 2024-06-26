package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Operator_Transform3 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3),
                                                   ("b", 4), ("b", 5), ("a", 6)
    ),2)
//    获取相同key的数据平均值
    val aggRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    val mapvRDD: RDD[(String, Int)] = aggRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    mapvRDD.collect().foreach(println)
    sc.stop()
  }

}
