package com.atguigu.bigdata.spark.core.RDD.operator.transdorm

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3),
                                                   ("b", 4), ("b", 5), ("a", 6)
    ),2)
    /*
    reduceByKey:
        combineByKeyWithClassTag[V](
          (v: V) => v,//第一个数据不参数计算
           func, //分区内的操作规则
           func, //分区间的操作规则
           )


    aggregateByKey:
        combineByKeyWithClassTag[U](
        (v: V) => cleanedSeqOp(createZero(), v),//初始值和第一个key的value值进行的分区内数据操作
          cleanedSeqOp,//分区内的操作规则
          combOp,//分区间的操作规则
          )


    foldByKey:
       combineByKeyWithClassTag[V](
       (v: V) => cleanedFunc(createZero(), v),//初始值和第一个key的value值进行的分区内数据操作
        cleanedFunc,  //分区内的操作规则
         cleanedFunc, //分区间的操作规则
          )

    combineByKey:
        combineByKeyWithClassTag(
        createCombiner, //对相同key的一个数据进行处理
        mergeValue,     //分区内的操作规则
        mergeCombiners, //分区间的操作规则
        )



     */
    rdd.reduceByKey(_+_)
    rdd.aggregateByKey(0)(_+_,_+_)
    rdd.foldByKey(0)(_+_)
    rdd.combineByKey(v=>v,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y)
    sc.stop()
  }

}
