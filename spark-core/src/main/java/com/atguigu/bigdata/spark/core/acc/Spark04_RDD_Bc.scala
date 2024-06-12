package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_RDD_Bc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val map=mutable.Map(("a",4),("b",5),("c",6))
//    封装广播变量
val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)
    rdd.map{
      case (w,c)=>{
        val l: Int = bc.value.getOrElse(w, 0)
        (w,(c,l))
      }
    }.collect().foreach(println)
    sc.stop()
  }

}