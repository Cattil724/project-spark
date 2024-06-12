package com.atguigu.bigdata.spark.sql2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSQLUdf {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("hive.metastore.uris","thrift://node1:9083")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //TODO 在DSL语句中使用UDF自定义
    //把小字母转成大写
    val to_Upper=udf(
      (name:String)=>{
        name.toUpperCase
      }
    )
    //使用DSL方式读取Hive数据
    spark.read
      .table("itheima.users")
      .select($"loginname",to_Upper($"loginname").as("names"))
      .show()
    println("================================================================")
    //TODO 在SQL语句中使用UDF自定义
    //把小字母转成大写
    spark.udf.register(
      "to_Upper",
      (name:String)=>{
        name.toUpperCase
      }
    )
    spark.sql(
      """
        |SELECT loginname,to_Upper(loginname) AS names FROM itheima.users
        |""".stripMargin)
      .show(10,truncate = false)
    spark.stop()
  }

}
