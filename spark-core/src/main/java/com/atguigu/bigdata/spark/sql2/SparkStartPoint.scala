package com.atguigu.bigdata.spark.sql2

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object SparkStartPoint {
  def main(args: Array[String]): Unit = {
      val sparkConf=new SparkConf().setAppName("test").setMaster("local")
      val spark= SparkSession.builder().config(sparkConf).getOrCreate()
      import spark.implicits._
    val inputDS: Dataset[String] = spark.read.textFile("data/wordcount.data")
     //获取Schema信息：字段名字和字段名字
      inputDS.printSchema()
     inputDS.show(10)
    spark.stop()
  }

}
