package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3),
                                                   ("b", 4), ("b", 5), ("a", 6)
    ),2)
//    获取相同key的数据平均值
//    第一个参数：将相同key的第一个数据进行结构的转换，实现操作
//    第二个参数：分区内的计算规则
//    第三个参数：分区间的计算规则
    val aggRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v=>(v,1),
      (t:(Int,Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1:(Int,Int), t2:(Int,Int)) => {
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
