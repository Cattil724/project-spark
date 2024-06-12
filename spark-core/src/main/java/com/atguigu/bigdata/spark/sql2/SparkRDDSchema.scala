package com.atguigu.bigdata.spark.sql2

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row,SparkSession }
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object SparkRDDSchema {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local").setAppName("test")
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val sc=spark.sparkContext
    val data: RDD[String] = sc.textFile("data/ml-100k/u.data")
    //Row
    val RowRDD= data.mapPartitions(
      line => line.map {
        x =>
          val Array(userId, itemId, rating, timestamp) = x.trim.split("\\t")
          Row(userId, itemId, rating.toDouble, timestamp.toLong)
      }
    )
    //Scheme
    val Scheme:StructType=StructType(
      Array(
        StructField("user_Id",StringType,nullable = true),
        StructField("item_Id",StringType,nullable = true),
        StructField("rating",DoubleType,nullable = true),
        StructField("timestamp",LongType,nullable = true)
      )
      )
   val df=spark.createDataFrame(RowRDD,Scheme)
    df.printSchema()
    df.show(20,truncate = false)
  }

}
