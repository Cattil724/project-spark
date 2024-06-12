package com.atguigu.bigdata.spark.sql2

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSDLWordCont {
  def main(args: Array[String]): Unit = {
      val sparkConf=new SparkConf().setAppName("test").setMaster("local")
      val spark= SparkSession.builder().config(sparkConf).getOrCreate()
      import spark.implicits._
      val inputDS: Dataset[String] = spark.read.textFile("data/wordcount.data")
//     //获取Schema信息：字段名字和字段名字
//      inputDS.printSchema()
    //trim：返回移除头尾空格的字符串
    val dataset: Dataset[String] = inputDS
      .filter(line => null != line && line.trim.length > 0)
      .flatMap(line => line.trim.split("\\s+"))
//    dataset.printSchema()
//    dataset.show()
    val resultDF: DataFrame = dataset.groupBy("value").count()
      resultDF.printSchema()
      resultDF.show(truncate = false)
    spark.stop()
  }

}
