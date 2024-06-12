package com.atguigu.bigdata.spark.sql2

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQLWordCont {
  def main(args: Array[String]): Unit = {
      val sparkConf=new SparkConf().setAppName("test").setMaster("local")
      val spark= SparkSession.builder().config(sparkConf).getOrCreate()
      import spark.implicits._
      val inputDS: Dataset[String] = spark.read.textFile("data/wordcount.data")
//     //获取Schema信息：字段名字和字段名字
//      inputDS.printSchema()
      //使用filter过滤NULL字段和空字段
    //trim：返回移除头尾空格的字符串
    val dataset: Dataset[String] = inputDS
      .filter(line => null != line && line.trim.length > 0)
      //使用flatMap对每一行进行分割
      .flatMap(line => line.trim.split("\\s+"))
//    dataset.printSchema()
//    dataset.show()
    dataset.createOrReplaceTempView("tb_words")
    //使用SQL语句执行
    val resultDF: DataFrame = spark.sql(
      """
        |select value,count(1) as total from tb_words group by value
        |""".stripMargin)
    resultDF.printSchema()
    resultDF.show()
    spark.stop()
  }

}
