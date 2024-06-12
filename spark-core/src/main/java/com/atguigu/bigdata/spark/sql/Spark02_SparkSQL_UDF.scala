package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkSQl")
    val spark= SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")
    //用户自定义函数
    spark.udf.register("prefixName",(name:String)=>{
      "Name:"+name
    })
    spark.sql("select age,prefixName(name) from user").show()
    //TODO 关闭环境
    spark.close()
  }


}
