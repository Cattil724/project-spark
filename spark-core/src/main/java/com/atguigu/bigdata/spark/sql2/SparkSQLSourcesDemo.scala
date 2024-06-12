package com.atguigu.bigdata.spark.sql2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object SparkSQLSourcesDemo {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    //TODO 1.parquet
    val usersDF: DataFrame = spark.read.parquet("data/resources/users.parquet")
//    usersDF.show()
    val loadDF: DataFrame = spark.read.load("data/resources/users.parquet")
//    loadDF.show()
    //TODO json
    val jsonDF: DataFrame = spark.read.json("data/resources/people.json")
    jsonDF.show(10)
    val textDF: DataFrame = spark.read.text("data/resources/people.json")
    textDF.select(
      get_json_object($"value","$.name").as("name"),
      get_json_object($"value","$.age").as("age")
    ).show(10)
    //TODO CSV
    val CSVDF: DataFrame = spark.read
      .option("header", "true")
      .option("sep", "\\t")
      .option("inferSchema", "true")
      .csv("data/ml-100k/u.dat")
      CSVDF.show(10,truncate = false)
      //TODO Mysql
    spark.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnic ode=true")
      .option("dbtable", "sdlgdb.employees")
      .option("user", "root")
      .option("password", "123456")
      .load()
      .show(10,truncate = false)

    //关闭
    spark.stop()
  }

}
