package com.atguigu.bigdata.spark.core.RDD.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc=new SparkContext(conf)
    val rdd= sc.makeRDD(List(1, 2, 3, 4))
    val user=new User()
    rdd.foreach(
      num=>{
        println("age="+(user.age+num))
      }
    )
    sc.stop()
  }
  //class User extends Serializable{
  case class User(){
    var age:Int=30
  }

}
