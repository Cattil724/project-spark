package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object Spark04_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {

    //TODO 创建SparkSQL的运行环境
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkSQl")
    val spark= SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://node1:3306/mysql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .load().show
    //TODO 关闭环境
    spark.close()
  }

}
