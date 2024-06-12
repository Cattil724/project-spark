package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_RDD_Acc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List("hello", "spark", "hello"))
    //    创建累加器
    val wcACC = new MyAccumulator()
    //    注册累加器
    sc.register(wcACC, "wordCountAcc")
    rdd.foreach(
      word => {
        wcACC.add(word)
      }
    )
    println(wcACC.value)
    sc.stop()
  }

  /*
  自定义累加器
  1.继承AccumulatorV2，定义泛型
  IN ：累加器输入的数据类型 String
  ONT ：累加器返回的数据类型  mutable.Map[String,Long]
  */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
    private var wcMap = mutable.Map[String, Long]()

    //   判断是否初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    //    获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCnt = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCnt)
    }
//  Driver合并累计器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1=this.wcMap
      val map2=other.value
      map2.foreach{
        case (word,count)=>{
          val newCount= map1.getOrElse(word,0L)+count
          map1.update(word,newCount)
      }
      }
    }
//  累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}