package com.atguigu.bigdata.spark.sql2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSQLHive {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("hive.metastore.uris","thrift://node1:9083")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //使用DSL方式读取Hive数据
    spark.read
      .table("myhive.score3")
      .groupBy($"cid")
      .agg(
        round(avg($"score"),2).as("avg_sc")
      )
      .show()
    println("==============================================================")
    //使用SQL语句实现
    spark.sql(
      """
        |select cid,round(sum(score)) as sum_sc from myhive.score3 group by cid
        |""".stripMargin)
      .show()

    spark.stop()
  }

}
