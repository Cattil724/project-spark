package com.atguigu.bigdata.spark.sql2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDInferring_test {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("SparkRDDInferring").setMaster("local")
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()
    val sc:SparkContext=spark.sparkContext
    import spark.implicits._
    val rdd: RDD[String] = sc.textFile("data/ml-100k/u.data")
    val df: RDD[MovieRating] = rdd.mapPartitions(line =>
      line.map {
        x =>
          val Array(userId, itemId, rating, timestamp) = x.trim.split("\\t")
          MovieRating(userId, itemId, rating.toDouble, timestamp.toLong)
      }
    )
    val newDF: DataFrame = df.toDF()
    newDF.printSchema()
    newDF.show(10)
    val newDS: Dataset[MovieRating] = df.toDS()
    newDS.printSchema()
    newDS.show(10,truncate = false)
    spark.stop()
  }

}
