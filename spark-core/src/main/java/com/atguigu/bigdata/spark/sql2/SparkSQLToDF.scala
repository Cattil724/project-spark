package com.atguigu.bigdata.spark.sql2

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLToDF {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("test")
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val df= spark.sparkContext.parallelize(
      Seq(
        (10001, "zhangsan", 23),
        (10002, "lisi", 22),
        (10003, "wangwu", 23),
        (10004, "zhaoliu", 24)
      ),
      //numSlices=2表示切片的数量为2。这可能可以理解为将数据或元素分成两个部分或切片进行处理。
      numSlices = 2
    ).toDF("id", "nema", "age")
    df.printSchema()
    df.show()
    println("============================================================")
    val newdf: DataFrame =Seq(
      (10001, "zhangsan", 23),
      (10002, "lisi", 22),
      (10003, "wangwu", 23),
      (10004, "zhaoliu", 24)
    ).toDF("id", "nema", "age")
    newdf.printSchema()
    newdf.show()
  }

}
